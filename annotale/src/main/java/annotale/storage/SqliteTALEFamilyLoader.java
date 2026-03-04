package annotale.storage;

import annotale.TALE;
import annotale.TALE.Repeat;
import annotale.TALEFamilyBuilder;
import annotale.TALEFamilyBuilder.TALEFamily;
import annotale.alignmentCosts.RVDCosts;
import de.jstacs.algorithms.alignment.Alignment.AlignmentType;
import de.jstacs.algorithms.alignment.cost.AffineCosts;
import de.jstacs.algorithms.alignment.cost.Costs;
import de.jstacs.clustering.hierachical.ClusterTree;
import de.jstacs.clustering.hierachical.Hclust;
import de.jstacs.data.alphabets.DNAAlphabetContainer;
import de.jstacs.data.sequences.Sequence;
import de.jstacs.io.NonParsableException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SqliteTALEFamilyLoader {

    private SqliteTALEFamilyLoader() {}

    public static TALEFamilyBuilder load(String dbPath) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            AnalysisConfig cfg = loadAnalysisConfig(conn);
            Map<Integer, TALE> taleById = loadTales(conn);
            TALEFamilyBuilder builder = new TALEFamilyBuilder(
                  cfg.costs, cfg.linkage, cfg.alignmentType,
                  cfg.extraGapOpen, cfg.extraGapExt, cfg.cut, cfg.pval,
                  cfg.reservedNames, cfg.dmat);
            Map<Integer, Integer> taleIndexById = loadTaleIndexMap(conn);
            TALEFamily[] families = loadFamilies(conn, builder, taleById, taleIndexById);
            builder.setFamilies(families);
            return builder;
        }
    }

    private static AnalysisConfig loadAnalysisConfig(Connection conn) throws Exception {
        AnalysisConfig cfg = new AnalysisConfig();
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT alignment_type, cut, extra_gap_open, extra_gap_ext, linkage, pval, "
                    + "cost_affine_open, cost_affine_extend, cost_rvd_gap, cost_rvd_twelve, cost_rvd_thirteen, cost_rvd_bonus, "
                    + "reserved_names FROM analysis_config WHERE id='default'")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    cfg.alignmentType = rs.getString(1) == null ? null : AlignmentType.valueOf(rs.getString(1));
                    cfg.cut = valueOrZero(rs.getObject(2));
                    cfg.extraGapOpen = valueOrZero(rs.getObject(3));
                    cfg.extraGapExt = valueOrZero(rs.getObject(4));
                    cfg.linkage = rs.getString(5) == null ? null : Hclust.Linkage.valueOf(rs.getString(5));
                    cfg.pval = valueOrZero(rs.getObject(6));
                    Double affineOpen = toDouble(rs.getObject(7));
                    Double affineExt = toDouble(rs.getObject(8));
                    Double rvdGap = toDouble(rs.getObject(9));
                    Double rvdTwelve = toDouble(rs.getObject(10));
                    Double rvdThirteen = toDouble(rs.getObject(11));
                    Double rvdBonus = toDouble(rs.getObject(12));
                    cfg.costs = buildCosts(affineOpen, affineExt, rvdGap, rvdTwelve, rvdThirteen, rvdBonus);
                    String reserved = rs.getString(13);
                    cfg.reservedNames = reserved == null || reserved.trim().isEmpty()
                          ? null : reserved.split(",");
                }
            }
        }
        cfg.dmat = loadDmat(conn);
        return cfg;
    }

    private static double[][] loadDmat(Connection conn) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT data FROM dmat WHERE id=1")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String data = rs.getString(1);
                    return data == null ? null : parseDmat(data);
                }
            }
        }
        return null;
    }

    private static double[][] parseDmat(String data) {
        int n = 0;
        int off = 0;
        while ((off = data.indexOf("$", off) + 1) > 0) {
            n++;
        }
        double[][] dmat = new double[n][];
        off = 0;
        int off2;
        int row = 0;
        while ((off2 = data.indexOf("$", off)) >= 0) {
            String[] parts = data.substring(off, off2).split(";");
            dmat[row] = new double[parts.length];
            for (int j = 0; j < parts.length; j++) {
                String val = parts[j].replace(",", ".");
                dmat[row][j] = Double.parseDouble(val);
            }
            row++;
            off = off2 + 1;
        }
        return dmat;
    }

    private static Costs buildCosts(Double affineOpen, Double affineExt, Double rvdGap,
          Double rvdTwelve, Double rvdThirteen, Double rvdBonus) {
        if (affineOpen == null || affineExt == null || rvdGap == null
              || rvdTwelve == null || rvdThirteen == null || rvdBonus == null) {
            return null;
        }
        RVDCosts rvd = new RVDCosts(rvdGap, rvdTwelve, rvdThirteen, rvdBonus);
        return new AffineCosts(affineOpen, affineExt, rvd);
    }

    private static Map<Integer, TALE> loadTales(Connection conn) throws Exception {
        Map<Integer, TALE> tales = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT t.id, t.legacy_name, t.dna_seq, t.protein_seq, t.start_pos, t.end_pos, t.strand, t.is_new, "
                    + "s.legacy_strain_name AS strain_name, a.accession AS acc_name, a.version AS acc_version "
                    + "FROM tale t "
                    + "LEFT JOIN assembly a ON a.id = t.assembly_id "
                    + "LEFT JOIN samples s ON s.id = a.sample_id")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int id = rs.getInt(1);
                    String name = rs.getString(2);
                    String dna = rs.getString(3);
                    String protein = rs.getString(4);
                    Integer startPos = (Integer) rs.getObject(5);
                    Integer endPos = (Integer) rs.getObject(6);
                    Boolean strand = rs.getObject(7) == null ? null : ((Number) rs.getObject(7)).intValue() >= 0;
                    boolean isNew = rs.getObject(8) != null && ((Number) rs.getObject(8)).intValue() != 0;
                    String strain = rs.getString(9);
                    String accName = rs.getString(10);
                    String accVersion = rs.getString(11);
                    String accession = accName == null ? null : accName + (accVersion == null || accVersion.isEmpty() ? "" : "." + accVersion);

                    Repeat[] repeats = loadRepeats(conn, id);
                    RepeatSplit split = computeRepeatSplit(protein, repeats);
                    TALE tale = buildTale(name, protein, repeats, isNew, split);
                    tale.setAnnotation(strain, accession, startPos, endPos, strand);
                    if (dna != null) {
                        TALE dnaTale = buildDnaTale(name, dna, repeats, split);
                        if (dnaTale != null) {
                            tale.setDnaOriginal(dnaTale);
                        }
                    }
                    tales.put(id, tale);
                }
            }
        }
        return tales;
    }

    private static Repeat[] loadRepeats(Connection conn, int taleId) throws Exception {
        List<Repeat> reps = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT rvd, rvd_len, masked_seq_1, masked_seq_2 FROM repeat WHERE tale_id=? ORDER BY repeat_ordinal")) {
            ps.setInt(1, taleId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String rvd = rs.getString(1);
                    Integer rvdLen = (Integer) rs.getObject(2);
                    String m1 = rs.getString(3);
                    String m2 = rs.getString(4);
                    String repeatSeq = buildRepeatSeq(m1, rvd, rvdLen, m2);
                    Sequence repeat = repeatSeq == null ? null : Sequence.create(annotale.Tools.Translator.DEFAULT.getProteinAlphabet(), repeatSeq);
                    reps.add(new Repeat(repeat));
                }
            }
        }
        return reps.toArray(new Repeat[0]);
    }

    private static String buildRepeatSeq(String masked1, String rvd, Integer rvdLen, String masked2) {
        if (masked1 == null && masked2 == null) {
            return null;
        }
        String rvdFrag = rvd;
        if (rvd != null && rvdLen != null && rvdLen == 1) {
            rvdFrag = rvd.substring(0, 1);
        }
        StringBuilder sb = new StringBuilder();
        if (masked1 != null) {
            sb.append(masked1);
        }
        if (rvdFrag != null) {
            sb.append(rvdFrag);
        }
        if (masked2 != null) {
            sb.append(masked2);
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    private static TALE buildTale(String name, String protein, Repeat[] repeats, boolean isNew,
          RepeatSplit split)
          throws Exception {
        if (protein == null || repeats == null || repeats.length == 0 || split == null) {
            return buildRvdTale(name, repeats, isNew);
        }
        String startStr = protein.substring(0, split.startIndex);
        String endStr = protein.substring(split.endIndex);
        Sequence start = startStr.isEmpty() ? null
              : Sequence.create(annotale.Tools.Translator.DEFAULT.getProteinAlphabet(), startStr);
        Sequence end = endStr.isEmpty() ? null
              : Sequence.create(annotale.Tools.Translator.DEFAULT.getProteinAlphabet(), endStr);
        if (start == null || end == null) {
            return buildRvdTale(name, repeats, isNew);
        }
        return new TALE(false, name, start, repeats, end, isNew);
    }

    private static TALE buildDnaTale(String name, String dna, Repeat[] proteinRepeats,
          RepeatSplit split) throws Exception {
        if (dna == null || proteinRepeats == null || proteinRepeats.length == 0 || split == null) {
            return null;
        }
        int startLen = split.startIndex * 3;
        int endLen = dna.length() - (split.endIndex * 3);
        int repeatLen = dna.length() - startLen - endLen;
        if (startLen < 0 || endLen < 0 || repeatLen < 0) {
            return null;
        }
        String startStr = dna.substring(0, Math.min(startLen, dna.length()));
        String repeatsStr = dna.substring(startLen, Math.min(startLen + repeatLen, dna.length()));
        String endStr = dna.substring(startLen + repeatLen);
        Sequence start = startStr.isEmpty() ? null : Sequence.create(DNAAlphabetContainer.SINGLETON, startStr);
        Sequence end = endStr.isEmpty() ? null : Sequence.create(DNAAlphabetContainer.SINGLETON, endStr);
        if (start == null || end == null) {
            return null;
        }
        Repeat[] dnaRepeats = buildDnaRepeats(repeatsStr, proteinRepeats);
        if (dnaRepeats == null) {
            return null;
        }
        return new TALE(false, name, start, dnaRepeats, end, false);
    }

    private static Repeat[] buildDnaRepeats(String repeatsStr, Repeat[] proteinRepeats) throws Exception {
        Repeat[] dnaRepeats = new Repeat[proteinRepeats.length];
        int offset = 0;
        for (int i = 0; i < proteinRepeats.length; i++) {
            Sequence prot = proteinRepeats[i].getRepeat();
            if (prot == null) {
                return null;
            }
            int len = prot.getLength() * 3;
            if (offset + len > repeatsStr.length()) {
                return null;
            }
            String sub = repeatsStr.substring(offset, offset + len);
            Sequence rep = Sequence.create(DNAAlphabetContainer.SINGLETON, sub);
            dnaRepeats[i] = new Repeat(rep);
            offset += len;
        }
        return dnaRepeats;
    }

    private static TALE buildRvdTale(String name, Repeat[] repeats, boolean isNew) throws Exception {
        if (repeats == null || repeats.length == 0) {
            return new TALE(name, Sequence.create(annotale.RVDAlphabetContainer.SINGLETON, "", "-"), false, isNew);
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < repeats.length; i++) {
            String rvd = repeats[i].getRvd();
            if (rvd == null) {
                rvd = "--";
            }
            if (i > 0) {
                sb.append("-");
            }
            sb.append(rvd);
        }
        Sequence rvds = Sequence.create(annotale.RVDAlphabetContainer.SINGLETON, sb.toString(), "-");
        return new TALE(name, rvds, false, isNew);
    }

    private static String concatRepeats(Repeat[] repeats) {
        StringBuilder sb = new StringBuilder();
        for (Repeat r : repeats) {
            if (r == null || r.getRepeat() == null) {
                return null;
            }
            sb.append(r.getRepeat().toString());
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    private static RepeatSplit computeRepeatSplit(String protein, Repeat[] repeats) {
        if (protein == null || repeats == null || repeats.length == 0) {
            return null;
        }
        String repeatConcat = concatRepeats(repeats);
        if (repeatConcat == null) {
            return null;
        }
        int idx = protein.indexOf(repeatConcat);
        if (idx < 0) {
            return null;
        }
        return new RepeatSplit(idx, idx + repeatConcat.length());
    }

    private static TALEFamily[] loadFamilies(Connection conn, TALEFamilyBuilder builder,
          Map<Integer, TALE> taleById, Map<Integer, Integer> taleIndexById) throws Exception {
        Map<String, List<TALE>> members = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT family_id, tale_id FROM tale_family_member ORDER BY family_id")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String familyId = rs.getString(1);
                    int taleId = rs.getInt(2);
                    TALE tale = taleById.get(taleId);
                    if (tale == null) {
                        continue;
                    }
                    members.computeIfAbsent(familyId, k -> new ArrayList<>()).add(tale);
                }
            }
        }
        Map<String, String> newickByFamily = loadFamilyNewick(conn);
        List<TALEFamily> families = new ArrayList<>();
        for (Map.Entry<String, List<TALE>> entry : members.entrySet()) {
            String newick = newickByFamily.get(entry.getKey());
            ClusterTree<TALE> tree = null;
            if (newick != null && !newick.trim().isEmpty()) {
                tree = ClusterTreeNewick.fromNewickWithTaleIds(newick, taleById, taleIndexById);
            }
            if (tree == null) {
                tree = buildSimpleTree(entry.getValue());
            }
            families.add(TALEFamilyBuilder.createFamily(entry.getKey(), tree, builder));
        }
        return families.toArray(new TALEFamily[0]);
    }

    private static ClusterTree<TALE> buildSimpleTree(List<TALE> tales) throws NonParsableException {
        List<ClusterTree<TALE>> nodes = new ArrayList<>();
        for (int i = 0; i < tales.size(); i++) {
            nodes.add(new ClusterTree<>(tales.get(i), i));
        }
        while (nodes.size() > 1) {
            ClusterTree<TALE> left = nodes.remove(0);
            ClusterTree<TALE> right = nodes.remove(0);
            nodes.add(new ClusterTree<>(0.0, -1, left, right));
        }
        return nodes.isEmpty() ? null : nodes.get(0);
    }

    private static double valueOrZero(Object value) {
        Double d = toDouble(value);
        return d == null ? 0.0 : d;
    }

    private static Double toDouble(Object value) {
        if (value == null) {
            return null;
        }
        return ((Number) value).doubleValue();
    }

    private static Map<String, String> loadFamilyNewick(Connection conn) throws Exception {
        Map<String, String> map = new HashMap<>();
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

    private static Map<Integer, Integer> loadTaleIndexMap(Connection conn) throws Exception {
        Map<Integer, Integer> map = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT ordinal, tale_id FROM dmat_tale_order ORDER BY ordinal")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int ordinal = rs.getInt(1);
                    int taleId = rs.getInt(2);
                    map.put(taleId, ordinal);
                }
            }
        }
        return map;
    }

    private static final class AnalysisConfig {
        AlignmentType alignmentType;
        Hclust.Linkage linkage;
        double cut;
        double extraGapOpen;
        double extraGapExt;
        double pval;
        Costs costs;
        String[] reservedNames;
        double[][] dmat;
    }

    private static final class RepeatSplit {
        final int startIndex;
        final int endIndex;

        private RepeatSplit(int startIndex, int endIndex) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }
    }
}
