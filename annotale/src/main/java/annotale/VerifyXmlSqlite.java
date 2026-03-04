package annotale;

import annotale.TALE.Repeat;
import de.jstacs.data.sequences.Sequence;
import de.jstacs.io.NonParsableException;

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

        TALEFamilyBuilder xmlBuilder = loadXml(xmlPath);
        Map<String, XmlTale> xmlTales = loadXmlTales(xmlBuilder);
        Map<String, Set<String>> xmlFamilies = loadXmlFamilies(xmlBuilder);

        DiffTracker diffs = new DiffTracker();

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            Map<Integer, DbTale> dbTales = loadDbTales(conn);
            Map<String, Set<String>> dbFamilies = loadDbFamilies(conn, dbTales);
            Map<String, String> dbFamilyNewick = loadDbFamilyNewick(conn);

            runCheck(diffs, "OK: TALEs and repeats match.", () -> compareTales(xmlTales, dbTales, diffs));
            runCheck(diffs, "OK: family membership matches.", () -> compareFamilies(xmlFamilies, dbFamilies, diffs));
            runCheck(diffs, "OK: family trees look consistent.", () -> verifyFamilyTrees(dbFamilies, dbFamilyNewick, diffs));
            runCheck(diffs, "OK: dmat rows and ordering match TALE count.", () -> verifyDmat(conn, dbTales.size(), diffs));
        }

        if (diffs.hasDiffs()) {
            System.err.println("Validation failed with " + diffs.getCount() + " mismatch(es).");
            System.exit(2);
        } else {
            System.out.println("Validation OK: XML and SQLite appear consistent.");
        }
    }

    private static TALEFamilyBuilder loadXml(String xmlPath) throws Exception {
        String raw = new String(Files.readAllBytes(Paths.get(xmlPath)));
        String normalized = normalizeLegacyXml(raw);
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
            info.strain = tale.getStrain();
            info.accession = tale.getAccession();
            info.startPos = tale.getStartPos();
            info.endPos = tale.getEndPos();
            info.strand = tale.getStrand();
            info.isNew = tale.isNew();
            info.repeats = extractRepeats(tale.getRepeats());
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
              "SELECT t.id, t.legacy_name, t.dna_seq, t.protein_seq, t.start_pos, t.end_pos, t.strand, t.is_new, "
                    + "s.legacy_strain_name AS strain_name, a.accession AS acc_name, a.version AS acc_version "
                    + "FROM tale t "
                    + "LEFT JOIN assembly a ON a.id = t.assembly_id "
                    + "LEFT JOIN samples s ON s.id = a.sample_id")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    DbTale t = new DbTale();
                    t.dbId = rs.getInt(1);
                    t.name = rs.getString(2);
                    t.dna = rs.getString(3);
                    t.protein = rs.getString(4);
                    t.startPos = (Integer) rs.getObject(5);
                    t.endPos = (Integer) rs.getObject(6);
                    t.strand = rs.getObject(7) == null ? null : ((Number) rs.getObject(7)).intValue() >= 0;
                    t.isNew = rs.getObject(8) != null && ((Number) rs.getObject(8)).intValue() != 0;
                    t.strain = rs.getString(9);
                    String accName = rs.getString(10);
                    String accVer = rs.getString(11);
                    t.accession = accName == null ? null : accName + (accVer == null || accVer.isEmpty() ? "" : "." + accVer);
                    t.repeats = loadDbRepeats(conn, t.dbId);
                    tales.put(t.dbId, t);
                }
            }
        }
        return tales;
    }

    private static List<DbRepeat> loadDbRepeats(Connection conn, int taleId) throws Exception {
        List<DbRepeat> reps = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT rvd, rvd_len, masked_seq_1, masked_seq_2 FROM repeat WHERE tale_id=? ORDER BY repeat_ordinal")) {
            ps.setInt(1, taleId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    DbRepeat r = new DbRepeat();
                    r.rvd = rs.getString(1);
                    r.rvdLen = (Integer) rs.getObject(2);
                    r.masked1 = rs.getString(3);
                    r.masked2 = rs.getString(4);
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
            diffField(diffs, id, "strain", xml.strain, db.strain);
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
            diffField(diffs, id + "#repeat" + i, "rvdLen", xr.rvdLen, dr.rvdLen);
            diffField(diffs, id + "#repeat" + i, "masked1", xr.masked1, dr.masked1);
            diffField(diffs, id + "#repeat" + i, "masked2", xr.masked2, dr.masked2);
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

    private static void verifyFamilyTrees(Map<String, Set<String>> dbFamilies,
          Map<String, String> dbFamilyNewick, DiffTracker diffs) {
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
            if (leafIds.size() != members.size()) {
                diffs.add("family_tree_leaf_count", familyId + " leaves=" + leafIds.size()
                      + " members=" + members.size());
            }
        }
    }

    private static void verifyDmat(Connection conn, int taleCount, DiffTracker diffs) throws Exception {
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
        if (dmat != null) {
            int rows = 0;
            int off = 0;
            while ((off = dmat.indexOf("$", off) + 1) > 0) {
                rows++;
            }
            if (rows != taleCount) {
                diffs.add("dmat_row_count", "rows=" + rows + " tales=" + taleCount);
            }
        }
    }

    private static List<XmlRepeat> extractRepeats(Repeat[] repeats) {
        List<XmlRepeat> list = new ArrayList<>();
        if (repeats == null) {
            return list;
        }
        for (Repeat r : repeats) {
            XmlRepeat xr = new XmlRepeat();
            xr.rvd = r == null ? null : r.getRvd();
            xr.rvdLen = r == null ? null : r.getRvdLength();
            Sequence[] masked = r == null ? null : r.getMaskedRepeats();
            xr.masked1 = masked != null && masked.length > 0 && masked[0] != null ? masked[0].toString() : null;
            xr.masked2 = masked != null && masked.length > 1 && masked[1] != null ? masked[1].toString() : null;
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
        Integer rvdLen;
        String masked1;
        String masked2;
    }

    private static final class DbTale {
        int dbId;
        String name;
        String protein;
        String dna;
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
        Integer rvdLen;
        String masked1;
        String masked2;
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
