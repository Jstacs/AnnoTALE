package annotale.storage;

import annotale.TALE;
import annotale.TALE.Repeat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

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
        TALE dnaOriginal = tale.getDnaOriginal();

        int taleId = insertSingleTale(tale, assemblyId, dnaOriginal);
        insertRepeats(taleId, tale.getRepeats());

        return taleId;
    }

    private int upsertSample(TALE tale) throws SQLException {
        String raw = tale == null ? null : tale.getStrain();
        if (raw == null || raw.trim().isEmpty()) {
            raw = "unknown";
        }
        TaxonParts sp = parseTaxon(raw);
        int taxonId = upsertTaxonomy(raw, sp);
        Integer existing = findSampleIdByLegacyName(raw);
        if (existing != null) {
            updateSampleDetails(existing, sp, taxonId);
            return existing;
        }
        try (PreparedStatement ps = conn.prepareStatement(
              "INSERT INTO samples(legacy_strain_name, strain_name, geo_tag, collection_date, biosample_id, taxon_id) "
                    + "VALUES (?,?,?,?,?,?)",
              Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, raw);
            ps.setString(2, sp == null ? null : sp.strain);
            ps.setObject(3, null);
            ps.setObject(4, null);
            ps.setObject(5, null);
            ps.setInt(6, taxonId);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (!rs.next()) {
                    throw new SQLException("Failed to retrieve generated sample id for " + raw);
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

    private void updateSampleDetails(int sampleId, TaxonParts sp, int taxonId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
              "UPDATE samples SET "
                    + "strain_name=COALESCE(strain_name, ?), "
                    + "taxon_id=COALESCE(taxon_id, ?) "
                    + "WHERE id=?")) {
            ps.setObject(1, sp == null ? null : sp.strain);
            ps.setInt(2, taxonId);
            ps.setInt(3, sampleId);
            ps.executeUpdate();
        }
    }

    private int insertSingleTale(TALE tale, int assemblyId, TALE dnaOriginal) throws SQLException {
        int taleId;
        boolean isPseudo = isPseudoName(tale == null ? null : tale.getId());
        try (PreparedStatement ps = conn.prepareStatement(
              "INSERT INTO tale(legacy_name,dna_seq,protein_seq,start_pos,end_pos,strand,is_new,is_pseudo,assembly_id) "
                    + "VALUES (?,?,?,?,?,?,?,?,?)",
              Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, tale.getId());
            ps.setString(2, buildDnaSequence(tale, dnaOriginal));
            ps.setString(3, buildSequenceString(tale));
            ps.setObject(4, tale.getStartPos());
            ps.setObject(5, tale.getEndPos());
            ps.setObject(6, tale.getStrand() == null ? null : (tale.getStrand() ? 1 : -1));
            ps.setObject(7, tale.isNew());
            ps.setObject(8, isPseudo ? 1 : 0);
            ps.setObject(9, assemblyId);
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

    private void insertRepeats(int taleId, Repeat[] baseRepeats) throws SQLException {
        if (baseRepeats == null) {
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement(
              "INSERT INTO repeat(tale_id, repeat_ordinal, rvd, rvd_pos, rvd_len, masked_seq_1, masked_seq_2) "
                    + "VALUES (?,?,?,?,?,?,?)")) {
            for (int i = 0; i < baseRepeats.length; i++) {
                Repeat base = baseRepeats[i];
                ps.setInt(1, taleId);
                ps.setInt(2, i);
                ps.setString(3, base == null ? null : base.getRvd());
                ps.setObject(4, base == null ? null : base.getRvdPosition());
                ps.setObject(5, base == null ? null : base.getRvdLength());
                String[] masked = maskedRepeats(base);
                ps.setString(6, masked[0]);
                ps.setString(7, masked[1]);
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
            return insertAssembly(null, null, null, sampleId);
        }
        AccessionParts parts = splitAccession(raw);
        Integer existing = findAssemblyIdByAccession(parts.name, parts.version);
        if (existing != null) {
            updateAssemblySample(existing, sampleId);
            return existing;
        }
        return insertAssembly(parts.name, parts.version, null, sampleId);
    }

    private Integer findAssemblyIdByAccession(String name, String version) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT id FROM assembly WHERE accession=? AND version=?")) {
            ps.setString(1, name);
            ps.setString(2, version);
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

    private int insertAssembly(String accession, String version, String repliconType, int sampleId)
          throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO assembly(accession, version, replicon_type, sample_id) VALUES (?,?,?,?)",
                     Statement.RETURN_GENERATED_KEYS)) {
            ps.setObject(1, accession);
            ps.setObject(2, version);
            ps.setObject(3, repliconType);
            ps.setInt(4, sampleId);
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

    private String buildSequenceString(TALE tale) {
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

    private String buildDnaSequence(TALE tale, TALE dnaOriginal) {
        if (dnaOriginal != null) {
            return buildSequenceString(dnaOriginal);
        }
        return buildSequenceString(tale);
    }

    private AccessionParts splitAccession(String raw) {
        String trimmed = raw.trim();
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

    private String[] maskedRepeats(Repeat r) {
        String[] masked = new String[]{null, null};
        if (r != null && r.getMaskedRepeats() != null) {
            if (r.getMaskedRepeats().length > 0 && r.getMaskedRepeats()[0] != null) {
                masked[0] = r.getMaskedRepeats()[0].toString();
            }
            if (r.getMaskedRepeats().length > 1 && r.getMaskedRepeats()[1] != null) {
                masked[1] = r.getMaskedRepeats()[1].toString();
            }
        }
        return masked;
    }

    private boolean isPseudoName(String raw) {
        return raw != null && raw.toLowerCase().contains("(pseudo)");
    }

    private TaxonParts parseTaxon(String raw) {
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
            String species = null;
            String pathovar = null;
            String strain = null;

        if (first.equals("x") || first.equals("x.") || first.equals("xanthomonas")) {
            if (tokens.length < 2) {
                return null;
            }
            species = "Xanthomonas " + tokens[1].toLowerCase();
                if (tokens[1].equalsIgnoreCase("sp.") || tokens[1].equalsIgnoreCase("sp")) {
                strain = normalizeStrainName(joinTokens(tokens, 2));
                return new TaxonParts(species, null, strain);
            }
            if (tokens.length >= 4 && isPvToken(tokens[2])) {
                pathovar = tokens[3].toLowerCase();
                strain = normalizeStrainName(joinTokens(tokens, 4));
            } else if (tokens.length >= 3 && tokens[2].matches("[A-Za-z]+")) {
                pathovar = tokens[2].toLowerCase();
                strain = normalizeStrainName(joinTokens(tokens, 3));
            } else {
                strain = normalizeStrainName(joinTokens(tokens, 2));
            }
            return new TaxonParts(species, pathovar, strain);
        }

        AbbrevTaxon abbrev = ABBREV_TAXA.get(first);
        if (abbrev != null) {
            species = abbrev.species;
            pathovar = abbrev.pathovar;
            int pvIndex = findPvIndex(tokens, 1);
            if (pvIndex >= 0 && pvIndex + 1 < tokens.length) {
                pathovar = tokens[pvIndex + 1].toLowerCase();
                strain = normalizeStrainName(joinTokens(tokens, pvIndex + 2));
            } else {
                strain = normalizeStrainName(joinTokens(tokens, 1));
            }
            return new TaxonParts(species, pathovar, strain);
        }

        return null;
    }

    private boolean isPvToken(String token) {
        String lower = token.toLowerCase();
        return lower.equals("pv") || lower.equals("pv.");
    }

    private int findPvIndex(String[] tokens, int startIndex) {
        for (int i = startIndex; i < tokens.length; i++) {
            if (isPvToken(tokens[i])) {
                return i;
            }
        }
        return -1;
    }

    private static final java.util.Map<String, AbbrevTaxon> ABBREV_TAXA = buildAbbrevTaxa();

    private static java.util.Map<String, AbbrevTaxon> buildAbbrevTaxa() {
        java.util.Map<String, AbbrevTaxon> map = new java.util.HashMap<>();
        map.put("xo", new AbbrevTaxon("Xanthomonas oryzae", null));
        map.put("xoo", new AbbrevTaxon("Xanthomonas oryzae", "oryzae"));
        map.put("xoc", new AbbrevTaxon("Xanthomonas oryzae", "oryzicola"));

        map.put("xt", new AbbrevTaxon("Xanthomonas translucens", null));
        map.put("xtt", new AbbrevTaxon("Xanthomonas translucens", "translucens"));
        map.put("xtu", new AbbrevTaxon("Xanthomonas translucens", "undulosa"));

        map.put("xp", new AbbrevTaxon("Xanthomonas phaseoli", null));
        map.put("xpp", new AbbrevTaxon("Xanthomonas phaseoli", "phaseoli"));
        map.put("xpm", new AbbrevTaxon("Xanthomonas phaseoli", "manihotis"));

        map.put("xc", new AbbrevTaxon("Xanthomonas citri", null));
        map.put("xca", new AbbrevTaxon("Xanthomonas citri", "anacardii"));
        map.put("xcc", new AbbrevTaxon("Xanthomonas campestris", "campestris"));
        map.put("xcf", new AbbrevTaxon("Xanthomonas campestris", "fuscans"));
        map.put("xcg", new AbbrevTaxon("Xanthomonas campestris", "glycines"));
        map.put("xcp", new AbbrevTaxon("Xanthomonas campestris", "punicae"));
        map.put("xcm", new AbbrevTaxon("Xanthomonas campestris", "musacearum"));
        map.put("xcv", new AbbrevTaxon("Xanthomonas campestris", "vesicatoria"));

        map.put("xg", new AbbrevTaxon("Xanthomonas gardneri", null));

        map.put("xa", new AbbrevTaxon("Xanthomonas anaxopodis", null));
        map.put("xav", new AbbrevTaxon("Xanthomonas anaxopodis", "vesicatoria"));
        map.put("xac", new AbbrevTaxon("Xanthomonas anaxopodis", "citri"));

        return map;
    }

    private String joinTokens(String[] tokens, int start) {
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

    private String normalizeStrainName(String raw) {
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
        return tokens[tokens.length - 1];
    }

    private static final class TaxonParts {
        final String species;
        final String pathovar;
        final String strain;

        private TaxonParts(String species, String pathovar, String strain) {
            this.species = species;
            this.pathovar = pathovar;
            this.strain = strain;
        }
    }

    private static final class AbbrevTaxon {
        final String species;
        final String pathovar;

        private AbbrevTaxon(String species, String pathovar) {
            this.species = species;
            this.pathovar = pathovar;
        }
    }

    private int upsertTaxonomy(String raw, TaxonParts sp) throws SQLException {
        String name = buildTaxonName(raw, sp);
        String rank = deriveTaxonRank(raw, sp);
        Integer existing = findTaxonomyId(name, sp);
        if (existing != null) {
            return existing;
        }
        try (PreparedStatement ps = conn.prepareStatement(
              "INSERT INTO taxonomy(ncbi_tax_id, rank, name, species, pathovar) "
                    + "VALUES (?,?,?,?,?)",
              Statement.RETURN_GENERATED_KEYS)) {
            ps.setObject(1, null);
            ps.setObject(2, rank);
            ps.setString(3, name);
            ps.setObject(4, sp == null ? null : sp.species);
            ps.setObject(5, sp == null ? null : sp.pathovar);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (!rs.next()) {
                    throw new SQLException("Failed to retrieve generated taxonomy id for " + name);
                }
                return rs.getInt(1);
            }
        }
    }

    private Integer findTaxonomyId(String name, TaxonParts sp) throws SQLException {
        if (sp != null && sp.species != null) {
            try (PreparedStatement ps = conn.prepareStatement(
                  "SELECT id FROM taxonomy WHERE name=? AND "
                        + "IFNULL(species,'')=IFNULL(?, '') AND IFNULL(pathovar,'')=IFNULL(?, '')")) {
                ps.setString(1, name);
                ps.setString(2, sp.species);
                ps.setString(3, sp.pathovar);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getInt(1);
                    }
                }
            }
            return null;
        }
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT id FROM taxonomy WHERE name=?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return null;
    }

    private String buildTaxonName(String raw, TaxonParts sp) {
        if (sp != null && sp.species != null) {
            if (sp.pathovar != null && !sp.pathovar.isEmpty()) {
                return sp.species + " pv. " + sp.pathovar;
            }
            return sp.species;
        }
        if (raw != null && !raw.trim().isEmpty()) {
            return raw.trim();
        }
        return "unknown";
    }

    private String deriveTaxonRank(String raw, TaxonParts sp) {
        if (sp != null) {
            if (sp.pathovar != null && !sp.pathovar.isEmpty()) {
                return "pathovar";
            }
            if (sp.species != null && !sp.species.isEmpty()) {
                return "species";
            }
        }
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        String[] tokens = trimmed.split("\\s+");
        if (tokens.length == 1 && tokens[0].equalsIgnoreCase("xanthomonas")) {
            return "genus";
        }
        if (tokens.length >= 2 && tokens[0].equalsIgnoreCase("xanthomonas")
              && (tokens[1].equalsIgnoreCase("sp.") || tokens[1].equalsIgnoreCase("sp"))) {
            return "genus";
        }
        return null;
    }
}
