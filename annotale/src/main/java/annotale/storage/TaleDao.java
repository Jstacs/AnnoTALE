package annotale.storage;

import annotale.TALE;
import annotale.TALE.Repeat;
import de.jstacs.data.sequences.Sequence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.Set;

public class TaleDao {

    private final Connection conn;

    public TaleDao(Connection conn) {
        this.conn = conn;
    }

    /**
     * Inserts a TALE and its repeats, returning the synthetic tale id.
     */
    public int insertTale(TALE tale) throws SQLException {
        int sampleId = upsertSample(tale);
        int assemblyId = upsertAssembly(tale, sampleId);
        int taleId = insertSingleTale(tale, assemblyId);
        insertRepeats(taleId, tale.getRepeats(), tale.getDnaOriginal() == null ? null : tale.getDnaOriginal().getRepeats());

        return taleId;
    }

    private int upsertSample(TALE tale) throws SQLException {
        String raw = tale == null ? null : tale.getStrain();
        if (raw == null || raw.trim().isEmpty()) {
            raw = "unknown";
        }
        String sampleName = normalizeSampleName(raw);
        String localStrain = parseStrain(sampleName);
        Integer existing = findSampleIdByLegacyName(sampleName);
        if (existing != null) {
            updateSampleDetails(existing, localStrain);
            return existing;
        }
        try (PreparedStatement ps = conn.prepareStatement(
              "INSERT INTO samples(biosample_id, legacy_strain_name, strain_name, strain_name_origin, "
                    + "geo_tag, collection_date, taxon_id) "
                    + "VALUES (?,?,?,?,?,?,?)",
              Statement.RETURN_GENERATED_KEYS)) {
            ps.setObject(1, null);
            ps.setString(2, sampleName);
            ps.setObject(3, localStrain);
            ps.setObject(4, localStrain == null ? null : "local");
            ps.setObject(5, null);
            ps.setObject(6, null);
            ps.setObject(7, null);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (!rs.next()) {
                    throw new SQLException("Failed to retrieve generated sample id for " + sampleName);
                }
                return rs.getInt(1);
            }
        }
    }

    private Integer findSampleIdByLegacyName(String name) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT id FROM samples WHERE legacy_strain_name=?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return null;
    }

    private void updateSampleDetails(int sampleId, String localStrain) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
              "UPDATE samples SET "
                    + "strain_name=COALESCE(strain_name, ?), "
                    + "strain_name_origin=CASE "
                    + "WHEN strain_name_origin IS NOT NULL THEN strain_name_origin "
                    + "WHEN strain_name IS NULL AND ? IS NOT NULL THEN 'local' "
                    + "ELSE strain_name_origin END "
                    + "WHERE id=?")) {
            ps.setString(1, localStrain);
            ps.setString(2, localStrain);
            ps.setInt(3, sampleId);
            ps.executeUpdate();
        }
    }

    private int insertSingleTale(TALE tale, int assemblyId) throws SQLException {
        int taleId;
        boolean isPseudo = isPseudoName(tale == null ? null : tale.getId());
        TALE dnaSource = tale.getDnaOriginal();
        try (PreparedStatement ps = conn.prepareStatement(
              "INSERT INTO tale(legacy_name,dna_start_seq,dna_end_seq,protein_start_seq,protein_end_seq,"
                    + "start_pos,end_pos,strand,is_new,is_pseudo,assembly_id) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
              Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, tale.getId());
            ps.setString(2, dnaSource == null ? null : sequenceString(dnaSource.getStart()));
            ps.setString(3, dnaSource == null ? null : sequenceString(dnaSource.getEnd()));
            ps.setString(4, sequenceString(tale.getStart()));
            ps.setString(5, sequenceString(tale.getEnd()));
            ps.setObject(6, tale.getStartPos());
            ps.setObject(7, tale.getEndPos());
            ps.setObject(8, tale.getStrand() == null ? null : (tale.getStrand() ? 1 : -1));
            ps.setObject(9, tale.isNew());
            ps.setObject(10, isPseudo ? 1 : 0);
            ps.setObject(11, assemblyId);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (!rs.next()) {
                    throw new SQLException("Failed to retrieve generated tale id for " + tale.getId());
                }
                taleId = rs.getInt(1);
            }
        }
        return taleId;
    }

    private void insertRepeats(int taleId, Repeat[] baseRepeats, Repeat[] dnaRepeats) throws SQLException {
        if (baseRepeats == null) {
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement(
              "INSERT INTO repeat(tale_id, repeat_ordinal, rvd, rvd_pos, rvd_len, protein_seq, dna_seq) "
                    + "VALUES (?,?,?,?,?,?,?)")) {
            for (int i = 0; i < baseRepeats.length; i++) {
                Repeat base = baseRepeats[i];
                ps.setInt(1, taleId);
                ps.setInt(2, i);
                ps.setString(3, base == null ? null : base.getRvd());
                ps.setObject(4, base == null ? null : base.getRvdPosition());
                ps.setObject(5, base == null ? null : base.getRvdLength());
                ps.setString(6, base == null ? null : sequenceString(base.getRepeat()));
                Repeat dna = dnaRepeats != null && i < dnaRepeats.length ? dnaRepeats[i] : null;
                ps.setString(7, dna == null ? null : sequenceString(dna.getRepeat()));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private int upsertAssembly(TALE tale, int sampleId) throws SQLException {
        String raw = tale == null ? null : tale.getAccession();
        if (raw == null || raw.trim().isEmpty() || "null".equalsIgnoreCase(raw.trim())) {
            Integer existing = findLocalAssemblyId(sampleId);
            if (existing != null) {
                return existing;
            }
            return insertAssembly(null, null, null, null, sampleId);
        }
        AccessionParts parts = splitAccession(raw);
        Integer existing = findAssemblyIdByAccession(parts.name, parts.version, sampleId);
        if (existing != null) {
            return existing;
        }
        Integer orphan = findAssemblyIdByAccession(parts.name, parts.version, null);
        if (orphan != null) {
            updateAssemblySample(orphan, sampleId);
            return orphan;
        }
        String accessionType = inferAccessionType(parts.name);
        return insertAssembly(parts.name, parts.version, accessionType, null, sampleId);
    }

    private Integer findAssemblyIdByAccession(String name, String version, Integer sampleId) throws SQLException {
        String sql = sampleId == null
              ? "SELECT id FROM assembly WHERE accession=? AND version=? AND sample_id IS NULL"
              : "SELECT id FROM assembly WHERE accession=? AND version=? AND sample_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, name);
            ps.setString(2, version);
            if (sampleId != null) {
                ps.setInt(3, sampleId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return null;
    }

    private Integer findLocalAssemblyId(int sampleId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT id FROM assembly WHERE sample_id=? AND accession IS NULL")) {
            ps.setInt(1, sampleId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return null;
    }

    private int insertAssembly(String accession, String version, String accessionType, String repliconType, int sampleId)
          throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO assembly(accession, version, accession_type, replicon_type, sample_id) "
                           + "VALUES (?,?,?,?,?)",
                     Statement.RETURN_GENERATED_KEYS)) {
            ps.setObject(1, accession);
            ps.setObject(2, version);
            ps.setObject(3, accessionType);
            ps.setObject(4, repliconType);
            ps.setInt(5, sampleId);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (!rs.next()) {
                    throw new SQLException("Failed to retrieve generated assembly id for " + accession);
                }
                return rs.getInt(1);
            }
        }
    }

    private void updateAssemblySample(int assemblyId, int sampleId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                     "UPDATE assembly SET sample_id=COALESCE(sample_id, ?) WHERE id=?")) {
            ps.setInt(1, sampleId);
            ps.setInt(2, assemblyId);
            ps.executeUpdate();
        }
    }

    private String inferAccessionType(String accession) {
        if (accession == null) {
            return null;
        }
        String trimmed = accession.trim().toUpperCase();
        if (trimmed.startsWith("GCA_") || trimmed.startsWith("GCF_")) {
            return "assembly";
        }
        return "nuccore";
    }

    private static String sequenceString(Sequence sequence) {
        return sequence == null ? null : sequence.toString();
    }

    private AccessionParts splitAccession(String raw) {
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return new AccessionParts("", "");
        }
        int dot = trimmed.lastIndexOf('.');
        if (dot > 0 && dot < trimmed.length() - 1) {
            String base = trimmed.substring(0, dot);
            String ver = trimmed.substring(dot + 1);
            if (ver.matches("\\d+")) {
                return new AccessionParts(base, ver);
            }
        }
        return new AccessionParts(trimmed, "");
    }

    private static final class AccessionParts {
        final String name;
        final String version;

        private AccessionParts(String name, String version) {
            this.name = name;
            this.version = version;
        }
    }

    private boolean isPseudoName(String raw) {
        return raw != null && raw.toLowerCase().contains("(pseudo)");
    }

    public static String normalizeSampleName(String raw) {
        if (raw == null) {
            return null;
        }
        return raw.trim().replaceFirst("(?i)\\s+plasmid\\s+unnamed\\d*$", "");
    }

    static String parseStrain(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return null;
        }

        String[] tokens = trimmed.split("\\s+");
        if (tokens.length == 0) {
            return null;
        }

        String first = tokens[0].toLowerCase();
        if (first.equals("x") || first.equals("x.") || first.equals("xanthomonas")) {
            if (tokens.length < 2) {
                return null;
            }
            int strainStart = 2;
            if (tokens[1].equalsIgnoreCase("sp.") || tokens[1].equalsIgnoreCase("sp")) {
                return normalizeStrainName(joinTokens(tokens, strainStart));
            }
            int pvIndex = findPvIndex(tokens, 2);
            if (pvIndex >= 0 && pvIndex + 1 < tokens.length) {
                strainStart = pvIndex + 2;
                if (strainStart + 1 < tokens.length && isVarToken(tokens[strainStart])) {
                    strainStart += 2;
                }
            } else if (tokens.length >= 4 && isSubspToken(tokens[2])) {
                strainStart = 4;
            }
            return normalizeStrainName(joinTokens(tokens, strainStart));
        }

        if (ABBREVIATED_TAXA.contains(first)) {
            int pvIndex = findPvIndex(tokens, 1);
            if (pvIndex >= 0 && pvIndex + 1 < tokens.length) {
                return normalizeStrainName(joinTokens(tokens, pvIndex + 2));
            }
            return normalizeStrainName(joinTokens(tokens, 1));
        }

        return null;
    }

    private static boolean isPvToken(String token) {
        String lower = token.toLowerCase();
        return lower.equals("pv") || lower.equals("pv.");
    }

    private static boolean isVarToken(String token) {
        String lower = token.toLowerCase();
        return lower.equals("var") || lower.equals("var.");
    }

    private static boolean isSubspToken(String token) {
        String lower = token.toLowerCase();
        return lower.equals("subsp") || lower.equals("subsp.");
    }

    private static int findPvIndex(String[] tokens, int startIndex) {
        for (int i = startIndex; i < tokens.length; i++) {
            if (isPvToken(tokens[i])) {
                return i;
            }
        }
        return -1;
    }

    private static final Set<String> ABBREVIATED_TAXA = Set.of(
          "xo", "xoo", "xoc", "xt", "xtt", "xtu", "xp", "xpp", "xpm", "xc", "xca", "xcc",
          "xcf", "xcg", "xcp", "xcm", "xcv", "xg", "xa", "xav", "xac");

    private static String joinTokens(String[] tokens, int start) {
        if (tokens == null || tokens.length <= start) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < tokens.length; i++) {
            if (sb.length() > 0) {
                sb.append(' ');
            }
            sb.append(tokens[i]);
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    private static String normalizeStrainName(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        return trimmed.replaceFirst("(?i)^(?:strain|str\\.)\\s+", "");
    }

}
