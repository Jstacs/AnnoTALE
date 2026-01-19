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
        int strainId = upsertStrain(tale);
        Integer accessionId = upsertAccession(tale, strainId);
        TALE dnaOriginal = tale.getDnaOriginal();

        int taleId = insertSingleTale(tale, strainId, accessionId, dnaOriginal);
        insertRepeats(taleId, tale.getRepeats());

        return taleId;
    }

    private int upsertStrain(TALE tale) throws SQLException {
        String raw = tale.getStrain();
        if (raw == null || raw.isEmpty()) {
            return ensureUnknownStrain();
        }
        Integer existing = findStrainIdByName(raw);
        if (existing != null) {
            return existing;
        }
        try (PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO strain(name, species, pathovar, isolate, geo_tag, tax_id) VALUES (?,?,?,?,?,?)",
                     Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, raw);
            TaxonParts sp = parseTaxon(raw);
            ps.setObject(2, sp == null ? null : sp.species);
            ps.setObject(3, sp == null ? null : sp.pathovar);
            ps.setString(4, sp == null ? null : sp.isolate);
            ps.setObject(5, null);
            ps.setObject(6, sp == null ? null : sp.taxId);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (!rs.next()) {
                    throw new SQLException("Failed to retrieve generated strain id for " + raw);
                }
                return rs.getInt(1);
            }
        }
    }

    private Integer findStrainIdByName(String name) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM strain WHERE name=?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return null;
    }

    private int ensureUnknownStrain() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                     "INSERT OR IGNORE INTO strain(name, isolate) VALUES ('unknown','unknown')")) {
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM strain WHERE name='unknown'")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        throw new SQLException("Failed to obtain unknown strain id");
    }

    private int insertSingleTale(TALE tale, int strainId, Integer accessionId, TALE dnaOriginal) throws SQLException {
        int taleId;
        NameParts parts = parseName(tale == null ? null : tale.getId());
        try (PreparedStatement ps = conn.prepareStatement(
              "INSERT INTO tale(name,name_short,name_suffix,is_pseudo,strain_id,accession_id,dna_seq,protein_seq,start_pos,end_pos,strand,is_new) "
                    + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
              Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, tale.getId());
            ps.setString(2, parts.shortName);
            ps.setString(3, parts.suffix);
            ps.setObject(4, parts.isPseudo ? 1 : 0);
            ps.setObject(5, strainId);
            ps.setObject(6, accessionId);
            ps.setString(7, buildDnaSequence(tale, dnaOriginal));
            ps.setString(8, buildSequenceString(tale));
            ps.setObject(9, tale.getStartPos());
            ps.setObject(10, tale.getEndPos());
            ps.setObject(11, tale.getStrand() == null ? null : (tale.getStrand() ? 1 : -1));
            ps.setObject(12, tale.isNew());
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
              "INSERT INTO repeat(tale_id, ordinal, rvd, rvd_pos, rvd_len, masked_seq_1, masked_seq_2) "
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

    private Integer upsertAccession(TALE tale, int strainId) throws SQLException {
        String raw = tale.getAccession();
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        AccessionParts parts = splitAccession(raw);
        Integer existing = findAccessionId(parts.name, parts.version);
        if (existing != null) {
            updateAccessionStrain(existing, strainId);
            return existing;
        }
        return insertAccession(parts, strainId);
    }

    private Integer findAccessionId(String name, String version) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT id FROM accession WHERE name=? AND version=?")) {
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

    private int insertAccession(AccessionParts parts, int strainId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO accession(name, version, replicon_type, strain_id) VALUES (?,?,?,?)",
                     Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, parts.name);
            ps.setString(2, parts.version);
            ps.setObject(3, null);
            ps.setInt(4, strainId);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (!rs.next()) {
                    throw new SQLException("Failed to retrieve generated accession id for " + parts.name);
                }
                return rs.getInt(1);
            }
        }
    }

    private void updateAccessionStrain(int accessionId, int strainId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                     "UPDATE accession SET strain_id=COALESCE(strain_id, ?) WHERE id=?")) {
            ps.setInt(1, strainId);
            ps.setInt(2, accessionId);
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

    private NameParts parseName(String raw) {
        if (raw == null) {
            return new NameParts(null, null, false);
        }
        boolean pseudo = raw.toLowerCase().contains("(pseudo)");
        String cleaned = raw.replaceAll("(?i)\\s*\\(pseudo\\)\\s*", "").trim();
        String shortName;
        String suffix = null;
        int firstSpace = cleaned.indexOf(' ');
        if (firstSpace >= 0) {
            shortName = cleaned.substring(0, firstSpace).trim();
            suffix = cleaned.substring(firstSpace + 1).trim();
            if (suffix.isEmpty()) {
                suffix = null;
            }
        } else {
            shortName = cleaned;
        }
        return new NameParts(shortName, suffix, pseudo);
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
        String isolate = null;

        if (first.equals("x") || first.equals("x.") || first.equals("xanthomonas")) {
            if (tokens.length < 2) {
                return null;
            }
            species = "Xanthomonas " + tokens[1].toLowerCase();
            if (tokens[1].equalsIgnoreCase("sp.") || tokens[1].equalsIgnoreCase("sp")) {
                isolate = normalizeIsolateFull(joinTokens(tokens, 2));
                return new TaxonParts(species, null, null, isolate);
            }
            if (tokens.length >= 4 && isPvToken(tokens[2])) {
                pathovar = tokens[3].toLowerCase();
                isolate = normalizeIsolateFull(joinTokens(tokens, 4));
            } else if (tokens.length >= 3 && tokens[2].matches("[A-Za-z]+")) {
                pathovar = tokens[2].toLowerCase();
                isolate = normalizeIsolateFull(joinTokens(tokens, 3));
            } else {
                isolate = normalizeIsolateFull(joinTokens(tokens, 2));
            }
            return new TaxonParts(species, pathovar, null, isolate);
        }

        AbbrevTaxon abbrev = ABBREV_TAXA.get(first);
        if (abbrev != null) {
            species = abbrev.species;
            pathovar = abbrev.pathovar;
            int pvIndex = findPvIndex(tokens, 1);
            if (pvIndex >= 0 && pvIndex + 1 < tokens.length) {
                pathovar = tokens[pvIndex + 1].toLowerCase();
                isolate = normalizeIsolateFull(joinTokens(tokens, pvIndex + 2));
            } else {
                isolate = normalizeIsolateFull(joinTokens(tokens, 1));
            }
            return new TaxonParts(species, pathovar, null, isolate);
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

    private String normalizeIsolateFull(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        String lower = trimmed.toLowerCase();
        if (lower.startsWith("strain ")) {
            trimmed = trimmed.substring(7).trim();
        }
        if (trimmed.isEmpty()) {
            return null;
        }
        return trimmed;
    }

    private static final class NameParts {
        final String shortName;
        final String suffix;
        final boolean isPseudo;

        private NameParts(String shortName, String suffix, boolean isPseudo) {
            this.shortName = shortName;
            this.suffix = suffix;
            this.isPseudo = isPseudo;
        }
    }

    private static final class TaxonParts {
        final String species;
        final String pathovar;
        final Integer taxId;
        final String isolate;

        private TaxonParts(String species, String pathovar, Integer taxId, String isolate) {
            this.species = species;
            this.pathovar = pathovar;
            this.taxId = taxId;
            this.isolate = isolate;
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

}
