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
                  null, cfg.dmat);
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
                    + "cost_affine_open, cost_affine_extend, cost_rvd_gap, cost_rvd_twelve, cost_rvd_thirteen, cost_rvd_bonus "
                    + "FROM analysis_config WHERE id='default'")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    cfg.alignmentType = rs.getString(1) == null ? null : AlignmentType.valueOf(rs.getString(1));
                    cfg.cut = ((Number) rs.getObject(2)).doubleValue();
                    cfg.extraGapOpen = ((Number) rs.getObject(3)).doubleValue();
                    cfg.extraGapExt = ((Number) rs.getObject(4)).doubleValue();
                    cfg.linkage = rs.getString(5) == null ? null : Hclust.Linkage.valueOf(rs.getString(5));
                    cfg.pval = ((Number) rs.getObject(6)).doubleValue();
                    Double affineOpen = toDouble(rs.getObject(7));
                    Double affineExt = toDouble(rs.getObject(8));
                    Double rvdGap = toDouble(rs.getObject(9));
                    Double rvdTwelve = toDouble(rs.getObject(10));
                    Double rvdThirteen = toDouble(rs.getObject(11));
                    Double rvdBonus = toDouble(rs.getObject(12));
                    cfg.costs = buildCosts(affineOpen, affineExt, rvdGap, rvdTwelve, rvdThirteen, rvdBonus);
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
              "SELECT t.id, t.legacy_name, t.dna_start_seq, t.dna_end_seq, t.protein_start_seq, t.protein_end_seq, "
                    + "t.start_pos, t.end_pos, t.strand, t.is_new, "
                    + "s.legacy_strain_name AS strain_name, a.accession AS acc_name, a.version AS acc_version "
                    + "FROM tale t "
                    + "LEFT JOIN assembly a ON a.id = t.assembly_id "
                    + "LEFT JOIN samples s ON s.id = a.sample_id")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int id = rs.getInt(1);
                    String name = rs.getString(2);
                    String dnaStart = rs.getString(3);
                    String dnaEnd = rs.getString(4);
                    String proteinStart = rs.getString(5);
                    String proteinEnd = rs.getString(6);
                    Integer startPos = (Integer) rs.getObject(7);
                    Integer endPos = (Integer) rs.getObject(8);
                    Boolean strand = rs.getObject(9) == null ? null : ((Number) rs.getObject(9)).intValue() >= 0;
                    boolean isNew = rs.getObject(10) != null && ((Number) rs.getObject(10)).intValue() != 0;
                    String strain = rs.getString(11);
                    String accName = rs.getString(12);
                    String accVersion = rs.getString(13);
                    String accession = accName == null ? null : accName + (accVersion == null || accVersion.isEmpty() ? "" : "." + accVersion);

                    Repeat[] repeats = loadRepeats(conn, id, false);
                    TALE tale = buildTale(name, proteinStart, proteinEnd, repeats, isNew);
                    tale.setAnnotation(strain, accession, startPos, endPos, strand);
                    if (dnaStart != null || dnaEnd != null) {
                        TALE dnaTale = buildDnaTale(name, dnaStart, dnaEnd, loadRepeats(conn, id, true));
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

    private static Repeat[] loadRepeats(Connection conn, int taleId, boolean dna) throws Exception {
        List<Repeat> reps = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT rvd, protein_seq, dna_seq FROM repeat WHERE tale_id=? ORDER BY repeat_ordinal")) {
            ps.setInt(1, taleId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String rvd = rs.getString(1);
                    String repeatSeq = rs.getString(dna ? 3 : 2);
                    Sequence repeat = repeatSeq == null ? null : Sequence.create(
                          dna ? DNAAlphabetContainer.SINGLETON : annotale.Tools.Translator.DEFAULT.getProteinAlphabet(), repeatSeq);
                    reps.add(new Repeat(repeat, rvd));
                }
            }
        }
        return reps.toArray(new Repeat[0]);
    }

    private static TALE buildTale(String name, String startString, String endString, Repeat[] repeats, boolean isNew)
          throws Exception {
        if (startString == null || endString == null || repeats == null || repeats.length == 0) {
            return buildRvdTale(name, repeats, isNew);
        }
        Sequence start = Sequence.create(annotale.Tools.Translator.DEFAULT.getProteinAlphabet(), startString);
        Sequence end = Sequence.create(annotale.Tools.Translator.DEFAULT.getProteinAlphabet(), endString);
        return new TALE(false, name, start, repeats, end, isNew);
    }

    private static TALE buildDnaTale(String name, String startString, String endString, Repeat[] repeats) throws Exception {
        if (startString == null || endString == null || repeats == null || repeats.length == 0) {
            return null;
        }
        Sequence start = Sequence.create(DNAAlphabetContainer.SINGLETON, startString);
        Sequence end = Sequence.create(DNAAlphabetContainer.SINGLETON, endString);
        return new TALE(false, name, start, repeats, end, false);
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
                throw new IllegalStateException("Missing family tree for " + entry.getKey());
            }
            families.add(TALEFamilyBuilder.createFamily(entry.getKey(), tree, builder));
        }
        return families.toArray(new TALEFamily[0]);
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
        double[][] dmat;
    }

}
