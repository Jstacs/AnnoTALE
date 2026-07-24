package annotale;

import annotale.TALE.Repeat;
import annotale.alignmentCosts.RVDCosts;
import annotale.storage.TaleDao;
import de.jstacs.algorithms.alignment.cost.AffineCosts;
import de.jstacs.algorithms.alignment.cost.Costs;
import de.jstacs.data.sequences.Sequence;
import de.jstacs.io.NonParsableException;
import de.jstacs.io.XMLParser;
import de.jstacs.utils.Pair;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class VerifyXmlSqlite {

    private static final int MAX_DIFFS = 50;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: VerifyXmlSqlite <class_definitions.xml> <sqlite.db>");
            System.exit(1);
        }

        String xmlPath = args[0];
        String dbPath = args[1];

        TALEFamilyBuilder xmlBuilder = loadXml(loadNormalizedXml(xmlPath));
        Map<String, XmlTale> xmlTales = loadXmlTales(xmlBuilder);
        Map<String, Set<String>> xmlFamilies = loadXmlFamilies(xmlBuilder);

        DiffTracker diffs = new DiffTracker();

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            Map<Integer, DbTale> dbTales = loadDbTales(conn);
            Map<String, Set<String>> dbFamilies = loadDbFamilies(conn, dbTales);
            Map<String, String> dbFamilyNewick = loadDbFamilyNewick(conn);

            runCheck(diffs, "OK: TALEs and repeats match.", () -> compareTales(xmlTales, dbTales, diffs));
            runCheck(diffs, "OK: family membership matches.", () -> compareFamilies(xmlFamilies, dbFamilies, diffs));
            runCheck(diffs, "OK: family alignments match.",
                  () -> compareFamilyAlignments(conn, xmlBuilder, dbTales, diffs));
            runCheck(diffs, "OK: family trees look consistent.", () -> verifyFamilyTrees(dbFamilies, dbFamilyNewick, dbTales, diffs));
            runCheck(diffs, "OK: dmat and ordering match.", () -> verifyDmat(conn, xmlBuilder, dbTales.size(), diffs));
            runCheck(diffs, "OK: analysis configuration matches.", () -> verifyAnalysisConfig(conn, xmlBuilder, diffs));
            runCheck(diffs, "OK: taxonomy references and lineage are consistent.", () -> verifyTaxonomy(conn, diffs));
        }

        if (diffs.hasDiffs()) {
            System.err.println("Validation failed with " + diffs.getCount() + " mismatch(es).");
            System.exit(2);
        } else {
            System.out.println("Validation OK: XML and SQLite appear consistent.");
        }
    }

    private static String loadNormalizedXml(String xmlPath) throws Exception {
        return normalizeLegacyXml(new String(Files.readAllBytes(Paths.get(xmlPath))));
    }

    private static TALEFamilyBuilder loadXml(String normalized) throws Exception {
        try {
            return new TALEFamilyBuilder(new StringBuffer(normalized));
        } catch (NonParsableException e) {
            throw new IllegalStateException("Failed to parse XML: " + e.getMessage(), e);
        }
    }

    private static Map<String, XmlTale> loadXmlTales(TALEFamilyBuilder builder) throws Exception {
        Map<String, XmlTale> map = new LinkedHashMap<>();
        TALE[] tales = builder.getAllTALEs();
        for (TALE tale : tales) {
            XmlTale info = new XmlTale();
            info.id = tale.getId();
            info.protein = buildSequenceString(tale);
            TALE dna = tale.getDnaOriginal();
            info.dna = dna == null ? null : buildSequenceString(dna);
            info.proteinStart = sequenceString(tale.getStart());
            info.proteinEnd = sequenceString(tale.getEnd());
            info.dnaStart = dna == null ? null : sequenceString(dna.getStart());
            info.dnaEnd = dna == null ? null : sequenceString(dna.getEnd());
            info.strain = tale.getStrain();
            info.accession = tale.getAccession();
            info.startPos = tale.getStartPos();
            info.endPos = tale.getEndPos();
            info.strand = tale.getStrand();
            info.isNew = tale.isNew();
            info.repeats = extractRepeats(tale.getRepeats(), dna == null ? null : dna.getRepeats());
            map.put(info.id, info);
        }
        return map;
    }

    private static Map<String, Set<String>> loadXmlFamilies(TALEFamilyBuilder builder) {
        Map<String, Set<String>> fams = new LinkedHashMap<>();
        TALEFamilyBuilder.TALEFamily[] families = builder.getFamilies();
        for (TALEFamilyBuilder.TALEFamily family : families) {
            Set<String> members = new HashSet<>();
            for (TALE tale : family.getFamilyMembers()) {
                members.add(tale.getId());
            }
            fams.put(family.getFamilyId(), members);
        }
        return fams;
    }

    private static Map<Integer, DbTale> loadDbTales(Connection conn) throws Exception {
        Map<Integer, DbTale> tales = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT t.id, t.legacy_name, t.dna_start_seq, t.dna_end_seq, t.protein_start_seq, t.protein_end_seq, "
                    + "t.start_pos, t.end_pos, t.strand, t.is_new, "
                    + "s.legacy_strain_name AS strain_name, a.accession AS acc_name, a.version AS acc_version "
                    + "FROM tale t "
                    + "LEFT JOIN assembly a ON a.id = t.assembly_id "
                    + "LEFT JOIN samples s ON s.id = a.sample_id")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    DbTale t = new DbTale();
                    t.dbId = rs.getInt(1);
                    t.name = rs.getString(2);
                    t.dnaStart = rs.getString(3);
                    t.dnaEnd = rs.getString(4);
                    t.proteinStart = rs.getString(5);
                    t.proteinEnd = rs.getString(6);
                    t.startPos = (Integer) rs.getObject(7);
                    t.endPos = (Integer) rs.getObject(8);
                    t.strand = rs.getObject(9) == null ? null : ((Number) rs.getObject(9)).intValue() >= 0;
                    t.isNew = rs.getObject(10) != null && ((Number) rs.getObject(10)).intValue() != 0;
                    t.strain = rs.getString(11);
                    String accName = rs.getString(12);
                    String accVer = rs.getString(13);
                    t.accession = accName == null ? null : accName + (accVer == null || accVer.isEmpty() ? "" : "." + accVer);
                    t.repeats = loadDbRepeats(conn, t.dbId);
                    t.protein = joinSequence(t.proteinStart, t.repeats, false, t.proteinEnd);
                    t.dna = joinSequence(t.dnaStart, t.repeats, true, t.dnaEnd);
                    tales.put(t.dbId, t);
                }
            }
        }
        return tales;
    }

    private static List<DbRepeat> loadDbRepeats(Connection conn, int taleId) throws Exception {
        List<DbRepeat> reps = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT rvd, rvd_pos, rvd_len, protein_seq, dna_seq "
                    + "FROM repeat WHERE tale_id=? ORDER BY repeat_ordinal")) {
            ps.setInt(1, taleId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    DbRepeat r = new DbRepeat();
                    r.rvd = rs.getString(1);
                    r.rvdPos = (Integer) rs.getObject(2);
                    r.rvdLen = (Integer) rs.getObject(3);
                    r.protein = rs.getString(4);
                    r.dna = rs.getString(5);
                    reps.add(r);
                }
            }
        }
        return reps;
    }

    private static Map<String, Set<String>> loadDbFamilies(Connection conn, Map<Integer, DbTale> dbTales)
          throws Exception {
        Map<String, Set<String>> fams = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT family_id, tale_id FROM tale_family_member ORDER BY family_id")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String familyId = rs.getString(1);
                    int taleId = rs.getInt(2);
                    DbTale tale = dbTales.get(taleId);
                    if (tale == null) {
                        continue;
                    }
                    fams.computeIfAbsent(familyId, k -> new HashSet<>()).add(tale.name);
                }
            }
        }
        return fams;
    }

    private static Map<String, String> loadDbFamilyNewick(Connection conn) throws Exception {
        Map<String, String> map = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT name, tree_newick FROM tale_family")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    map.put(rs.getString(1), rs.getString(2));
                }
            }
        }
        return map;
    }

    private static void compareTales(Map<String, XmlTale> xmlTales,
          Map<Integer, DbTale> dbTales, DiffTracker diffs) {
        Map<String, DbTale> dbByName = new HashMap<>();
        for (DbTale t : dbTales.values()) {
            dbByName.put(t.name, t);
        }
        if (xmlTales.size() != dbByName.size()) {
            diffs.add("tale_count", "xml=" + xmlTales.size() + " db=" + dbByName.size());
        }
        for (Map.Entry<String, XmlTale> entry : xmlTales.entrySet()) {
            String id = entry.getKey();
            XmlTale xml = entry.getValue();
            DbTale db = dbByName.get(id);
            if (db == null) {
                diffs.add("tale_missing_db", id);
                continue;
            }
            diffField(diffs, id, "protein", xml.protein, db.protein);
            diffField(diffs, id, "dna", xml.dna, db.dna);
            diffField(diffs, id, "proteinStart", xml.proteinStart, db.proteinStart);
            diffField(diffs, id, "proteinEnd", xml.proteinEnd, db.proteinEnd);
            diffField(diffs, id, "dnaStart", xml.dnaStart, db.dnaStart);
            diffField(diffs, id, "dnaEnd", xml.dnaEnd, db.dnaEnd);
            diffField(diffs, id, "strain", TaleDao.normalizeSampleName(xml.strain), db.strain);
            diffField(diffs, id, "accession", xml.accession, db.accession);
            diffField(diffs, id, "startPos", xml.startPos, db.startPos);
            diffField(diffs, id, "endPos", xml.endPos, db.endPos);
            diffField(diffs, id, "strand", xml.strand, db.strand);
            diffField(diffs, id, "isNew", xml.isNew, db.isNew);
            compareRepeats(diffs, id, xml.repeats, db.repeats);
        }
    }

    private static void compareRepeats(DiffTracker diffs, String id, List<XmlRepeat> xml,
          List<DbRepeat> db) {
        if (xml.size() != db.size()) {
            diffs.add("repeat_count", id + " xml=" + xml.size() + " db=" + db.size());
            return;
        }
        for (int i = 0; i < xml.size(); i++) {
            XmlRepeat xr = xml.get(i);
            DbRepeat dr = db.get(i);
            diffField(diffs, id + "#repeat" + i, "rvd", xr.rvd, dr.rvd);
            diffField(diffs, id + "#repeat" + i, "rvdPos", xr.rvdPos, dr.rvdPos);
            diffField(diffs, id + "#repeat" + i, "rvdLen", xr.rvdLen, dr.rvdLen);
            diffField(diffs, id + "#repeat" + i, "protein", xr.protein, dr.protein);
            diffField(diffs, id + "#repeat" + i, "dna", xr.dna, dr.dna);
        }
    }

    private static void compareFamilies(Map<String, Set<String>> xmlFamilies,
          Map<String, Set<String>> dbFamilies, DiffTracker diffs) {
        if (xmlFamilies.size() != dbFamilies.size()) {
            diffs.add("family_count", "xml=" + xmlFamilies.size() + " db=" + dbFamilies.size());
        }
        for (Map.Entry<String, Set<String>> entry : xmlFamilies.entrySet()) {
            String id = entry.getKey();
            Set<String> xmlMembers = entry.getValue();
            Set<String> dbMembers = dbFamilies.get(id);
            if (dbMembers == null) {
                diffs.add("family_missing_db", id);
                continue;
            }
            if (!xmlMembers.equals(dbMembers)) {
                diffs.add("family_members", id + " xml=" + xmlMembers.size() + " db=" + dbMembers.size());
            }
        }
    }

    private static void compareFamilyAlignments(Connection conn, TALEFamilyBuilder builder,
          Map<Integer, DbTale> dbTales, DiffTracker diffs) throws Exception {
        Map<String, Integer> idsByName = new HashMap<>();
        for (DbTale tale : dbTales.values()) {
            idsByName.put(tale.name, tale.dbId);
        }
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT alignment_fasta FROM tale_family WHERE name=?")) {
            for (TALEFamilyBuilder.TALEFamily family : builder.getFamilies()) {
                String expected = toFasta(family.getInducedMultipleAlignment(), idsByName);
                ps.setString(1, family.getFamilyId());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next() || rs.getString(1) == null) {
                        diffs.add("family_alignments_missing", family.getFamilyId());
                    } else if (!expected.equals(rs.getString(1))) {
                        diffs.add("family_alignments", family.getFamilyId());
                    }
                }
            }
        }
    }

    private static String toFasta(Pair<TALE[], String[]> alignment, Map<String, Integer> taleIdMap) {
        StringBuilder fasta = new StringBuilder();
        TALE[] tales = alignment.getFirstElement();
        String[] rows = alignment.getSecondElement();
        for (int i = 0; i < tales.length; i++) {
            fasta.append('>').append(taleIdMap.get(tales[i].getId())).append('\n');
            fasta.append(rows[i].replace(" ", "")).append('\n');
        }
        return fasta.toString();
    }

    private static void verifyFamilyTrees(Map<String, Set<String>> dbFamilies,
          Map<String, String> dbFamilyNewick, Map<Integer, DbTale> dbTales, DiffTracker diffs) {
        for (Map.Entry<String, Set<String>> entry : dbFamilies.entrySet()) {
            String familyId = entry.getKey();
            Set<String> members = entry.getValue();
            String newick = dbFamilyNewick.get(familyId);
            if (newick == null || newick.trim().isEmpty()) {
                diffs.add("family_tree_missing", familyId);
                continue;
            }
            Set<Integer> leafIds = extractLeafIds(newick);
            if (leafIds.isEmpty()) {
                diffs.add("family_tree_empty", familyId);
                continue;
            }
            Set<String> leafNames = new HashSet<>();
            for (Integer leafId : leafIds) {
                DbTale tale = dbTales.get(leafId);
                if (tale != null) {
                    leafNames.add(tale.name);
                }
            }
            if (!leafNames.equals(members)) {
                diffs.add("family_tree_leaves", familyId + " leaves=" + leafNames.size()
                      + " members=" + members.size());
            }
        }
    }

    private static void verifyDmat(Connection conn, TALEFamilyBuilder builder, int taleCount, DiffTracker diffs) throws Exception {
        int orderCount = 0;
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT COUNT(*) FROM dmat_tale_order")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    orderCount = rs.getInt(1);
                }
            }
        }
        if (orderCount != taleCount) {
            diffs.add("dmat_tale_order_count", "order=" + orderCount + " tales=" + taleCount);
        }
        String dmat = null;
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT data FROM dmat WHERE id=1")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    dmat = rs.getString(1);
                }
            }
        }
        if (dmat == null) {
            diffs.add("dmat_missing", "missing dmat row");
        } else {
            int rows = 0;
            int off = 0;
            while ((off = dmat.indexOf("$", off) + 1) > 0) {
                rows++;
            }
            if (rows != taleCount) {
                diffs.add("dmat_row_count", "rows=" + rows + " tales=" + taleCount);
            }
            String expected = extractDmat(builder.toXML());
            if (!dmat.equals(expected)) {
                diffs.add("dmat_values", "stored dmat differs from XML");
            }
        }
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT COUNT(*) FROM (SELECT tale_id FROM dmat_tale_order GROUP BY tale_id HAVING COUNT(*) > 1)");
             ResultSet rs = ps.executeQuery()) {
            if (rs.next() && rs.getInt(1) != 0) {
                diffs.add("dmat_tale_order_duplicates", "a tale appears more than once");
            }
        }
    }

    private static void verifyTaxonomy(Connection conn, DiffTracker diffs) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("PRAGMA foreign_key_check");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                diffs.add("foreign_key", rs.getString(1) + " rowid=" + rs.getLong(2));
            }
        }
        for (String issue : taxonomyIssues(conn)) {
            diffs.add("taxonomy", issue);
        }
    }

    static List<String> taxonomyIssues(Connection conn) throws Exception {
        Map<Integer, Integer> parents = new HashMap<>();
        List<String> issues = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT id, parent_id FROM taxonomy");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                int id = rs.getInt(1);
                Integer parentId = rs.getObject(2) == null ? null : rs.getInt(2);
                parents.put(id, parentId);
            }
        }
        for (Map.Entry<Integer, Integer> entry : parents.entrySet()) {
            Integer parentId = entry.getValue();
            if (parentId != null && !parents.containsKey(parentId)) {
                issues.add("missing taxonomy parent child_id=" + entry.getKey() + " parent_id=" + parentId);
            }
        }
        Set<Integer> checked = new HashSet<>();
        for (Integer id : parents.keySet()) {
            if (checked.contains(id)) {
                continue;
            }
            Set<Integer> path = new HashSet<>();
            Integer current = id;
            while (current != null && parents.containsKey(current) && path.add(current)) {
                checked.add(current);
                current = parents.get(current);
            }
            if (current != null && path.contains(current)) {
                issues.add("taxonomy cycle at id=" + current);
            }
        }
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT s.id, s.taxon_id FROM samples s LEFT JOIN taxonomy t ON t.id=s.taxon_id "
                    + "WHERE s.taxon_id IS NOT NULL AND t.id IS NULL");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                issues.add("missing sample taxon sample_id=" + rs.getInt(1) + " taxon_id=" + rs.getInt(2));
            }
        }
        return issues;
    }

    private static void verifyAnalysisConfig(Connection conn, TALEFamilyBuilder builder, DiffTracker diffs) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT alignment_type,cut,extra_gap_open,extra_gap_ext,linkage,pval,cost_affine_open,cost_affine_extend,"
                    + "cost_rvd_gap,cost_rvd_twelve,cost_rvd_thirteen,cost_rvd_bonus FROM analysis_config WHERE id='default'");
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                diffs.add("analysis_config", "missing default configuration");
                return;
            }
            diffField(diffs, "analysis_config", "alignment_type", builder.getAlignmentType().name(), rs.getString(1));
            diffField(diffs, "analysis_config", "cut", builder.getCut(), rs.getDouble(2));
            diffField(diffs, "analysis_config", "extra_gap_open", builder.getExtraGapOpening(), rs.getDouble(3));
            diffField(diffs, "analysis_config", "extra_gap_ext", builder.getExtraGapExtension(), rs.getDouble(4));
            diffField(diffs, "analysis_config", "linkage", builder.getLinkage().name(), rs.getString(5));
            diffField(diffs, "analysis_config", "pval", builder.getPVal(), rs.getDouble(6));
            Costs costs = builder.getCosts();
            if (!(costs instanceof AffineCosts) || !(((AffineCosts) costs).getInternalCosts() instanceof RVDCosts)) {
                diffs.add("analysis_config", "unsupported XML cost model");
                return;
            }
            AffineCosts affine = (AffineCosts) costs;
            RVDCosts rvd = (RVDCosts) affine.getInternalCosts();
            diffField(diffs, "analysis_config", "affine_open", affine.getInsertCosts(), rs.getDouble(7));
            diffField(diffs, "analysis_config", "affine_extend", affine.getElongateInsertCosts(), rs.getDouble(8));
            diffField(diffs, "analysis_config", "rvd_gap", rvd.getGapCosts(), rs.getDouble(9));
            diffField(diffs, "analysis_config", "rvd_twelve", rvd.getTwelve(), rs.getDouble(10));
            diffField(diffs, "analysis_config", "rvd_thirteen", rvd.getThirteen(), rs.getDouble(11));
            diffField(diffs, "analysis_config", "rvd_bonus", rvd.getBonus(), rs.getDouble(12));
        }
    }

    private static String extractDmat(StringBuffer builderXml) {
        try {
            String value = XMLParser.extractForTag(new StringBuffer(builderXml), "dmatStore").toString();
            int start = value.indexOf("<dmatStore>");
            int end = value.lastIndexOf("</dmatStore>");
            return start >= 0 && end > start ? value.substring(start + 11, end) : value;
        } catch (Exception e) {
            return null;
        }
    }

    private static List<XmlRepeat> extractRepeats(Repeat[] repeats, Repeat[] dnaRepeats) {
        List<XmlRepeat> list = new ArrayList<>();
        if (repeats == null) {
            return list;
        }
        for (int i = 0; i < repeats.length; i++) {
            Repeat r = repeats[i];
            XmlRepeat xr = new XmlRepeat();
            xr.rvd = r == null ? null : r.getRvd();
            xr.rvdPos = r == null ? null : r.getRvdPosition();
            xr.rvdLen = r == null ? null : r.getRvdLength();
            xr.protein = r == null ? null : sequenceString(r.getRepeat());
            Repeat dna = dnaRepeats != null && i < dnaRepeats.length ? dnaRepeats[i] : null;
            xr.dna = dna == null ? null : sequenceString(dna.getRepeat());
            list.add(xr);
        }
        return list;
    }

    private static String buildSequenceString(TALE tale) {
        if (tale == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        if (tale.getStart() != null) {
            sb.append(tale.getStart().toString());
        }
        if (tale.getRepeats() != null) {
            for (Repeat r : tale.getRepeats()) {
                if (r != null && r.getRepeat() != null) {
                    sb.append(r.getRepeat().toString());
                }
            }
        }
        if (tale.getEnd() != null) {
            sb.append(tale.getEnd().toString());
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    private static String sequenceString(Sequence sequence) {
        return sequence == null ? null : sequence.toString();
    }

    private static String joinSequence(String start, List<DbRepeat> repeats, boolean dna, String end) {
        StringBuilder sequence = new StringBuilder();
        if (start != null) {
            sequence.append(start);
        }
        for (DbRepeat repeat : repeats) {
            String part = dna ? repeat.dna : repeat.protein;
            if (part != null) {
                sequence.append(part);
            }
        }
        if (end != null) {
            sequence.append(end);
        }
        return sequence.length() == 0 ? null : sequence.toString();
    }

    private static Set<Integer> extractLeafIds(String newick) {
        Set<Integer> ids = new HashSet<>();
        int i = 0;
        char lastSig = '\0';
        while (i < newick.length()) {
            char c = newick.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }
            if (isDelimiter(c)) {
                lastSig = c;
                i++;
                if (c == ':') {
                    i = skipBranchLength(newick, i);
                }
                continue;
            }
            int start = i;
            while (i < newick.length() && !isDelimiter(newick.charAt(i))) {
                i++;
            }
            String token = newick.substring(start, i).trim();
            if (!token.isEmpty() && isDigits(token) && lastSig != ')') {
                ids.add(Integer.parseInt(token));
            }
            lastSig = '\0';
        }
        return ids;
    }

    private static boolean isDelimiter(char c) {
        return c == ',' || c == '(' || c == ')' || c == ':' || c == ';' || Character.isWhitespace(c);
    }

    private static int skipBranchLength(String newick, int i) {
        while (i < newick.length()) {
            char c = newick.charAt(i);
            if (c == ',' || c == ')' || c == ';') {
                break;
            }
            i++;
        }
        return i;
    }

    private static boolean isDigits(String token) {
        for (int i = 0; i < token.length(); i++) {
            if (!Character.isDigit(token.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static void runCheck(DiffTracker diffs, String msg, CheckedRunnable check) throws Exception {
        int before = diffs.getCount();
        check.run();
        if (diffs.getCount() == before) {
            System.out.println(msg);
        }
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run() throws Exception;
    }

    private static String normalizeLegacyXml(String xml) {
        return xml.replace("projects.xanthogenomes.", "annotale.");
    }

    private static void diffField(DiffTracker diffs, String id, String field, Object a, Object b) {
        if (a == null && b == null) {
            return;
        }
        if (a == null || !a.equals(b)) {
            diffs.add("tale_field", id + " " + field + " xml=" + a + " db=" + b);
        }
    }

    private static final class XmlTale {
        String id;
        String protein;
        String dna;
        String proteinStart;
        String proteinEnd;
        String dnaStart;
        String dnaEnd;
        String strain;
        String accession;
        Integer startPos;
        Integer endPos;
        Boolean strand;
        boolean isNew;
        List<XmlRepeat> repeats;
    }

    private static final class XmlRepeat {
        String rvd;
        Integer rvdPos;
        Integer rvdLen;
        String protein;
        String dna;
    }

    private static final class DbTale {
        int dbId;
        String name;
        String protein;
        String dna;
        String proteinStart;
        String proteinEnd;
        String dnaStart;
        String dnaEnd;
        String strain;
        String accession;
        Integer startPos;
        Integer endPos;
        Boolean strand;
        boolean isNew;
        List<DbRepeat> repeats;
    }

    private static final class DbRepeat {
        String rvd;
        Integer rvdPos;
        Integer rvdLen;
        String protein;
        String dna;
    }

    private static final class DiffTracker {
        private int count = 0;

        void add(String kind, String msg) {
            if (count < MAX_DIFFS) {
                System.err.println("DIFF [" + kind + "] " + msg);
            } else if (count == MAX_DIFFS) {
                System.err.println("... more mismatches omitted ...");
            }
            count++;
        }

        boolean hasDiffs() {
            return count > 0;
        }

        int getCount() {
            return count;
        }
    }
}
