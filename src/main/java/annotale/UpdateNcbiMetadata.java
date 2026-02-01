package annotale;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UpdateNcbiMetadata {

    private static final int TIMEOUT_MS = 15000;
    private static final String USER_AGENT = "AnnoTALE/1.0 (NCBI bulk enrichment)";
    private static final int DEFAULT_DELAY_MS = 200;
    private static final int DEFAULT_BATCH_SIZE = 100;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: UpdateNcbiMetadata <sqlite.db>");
            System.exit(1);
        }
        String dbPath = args[0];
        int batchSize = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_BATCH_SIZE;
        File cacheDir = new File(System.getenv().getOrDefault("ANNOTTALE_NCBI_CACHE_DIR", "/tmp/annotale-ncbi-cache"));
        if (!cacheDir.exists() && !cacheDir.mkdirs()) {
            System.err.println("NCBI: failed to create cache dir " + cacheDir.getAbsolutePath());
            System.exit(2);
        }
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            conn.setAutoCommit(true);
            Map<String, List<AssemblyTarget>> targets = loadTargets(conn);
            List<String> accessions = new ArrayList<>(targets.keySet());
            Collections.sort(accessions);
            int total = 0;
            int totalTargets = 0;
            for (List<AssemblyTarget> list : targets.values()) {
                totalTargets += list.size();
            }
            int totalBatches = (accessions.size() + batchSize - 1) / batchSize;
            for (int i = 0; i < accessions.size(); i += batchSize) {
                int end = Math.min(accessions.size(), i + batchSize);
                List<String> batch = accessions.subList(i, end);
                int batchIndex = (i / batchSize) + 1;
                System.out.println("NCBI: batch " + batchIndex + "/" + totalBatches
                      + " accessions=" + batch.size()
                      + " processed=" + total + "/" + totalTargets);

                BatchInputs inputs = buildBatchInputs(batch);
                Map<String, GenbankData> parsedRecords =
                      loadParsedRecords(inputs, targets, cacheDir, batchSize);
                Map<String, String> biosampleXml =
                      loadBatchBiosamples(parsedRecords, cacheDir, batchSize, batchIndex, totalBatches);
                total += processBatch(conn, cacheDir, batch, parsedRecords, biosampleXml, targets);
            }
            System.out.println("NCBI: finished enrichment for " + total + " assemblies.");
        }
    }

    private static Map<String, List<AssemblyTarget>> loadTargets(Connection conn) throws Exception {
        Map<String, List<AssemblyTarget>> targets = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT a.id, a.accession, a.version, a.accession_type, a.replicon_type, a.sample_id, "
                    + "s.legacy_strain_name, lt.name, lt.species, lt.pathovar "
                    + "FROM assembly a "
                    + "LEFT JOIN samples s ON s.id = a.sample_id "
                    + "LEFT JOIN taxonomy_legacy lt ON lt.id = s.legacy_taxon_id "
                    + "WHERE a.accession IS NOT NULL AND "
                    + "(a.accession_type IS NULL OR a.replicon_type IS NULL OR s.biosample_id IS NULL "
                    + "OR s.geo_tag IS NULL OR s.collection_date IS NULL OR s.strain_name IS NULL "
                    + "OR s.taxon_id IS NULL)")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    AssemblyTarget t = new AssemblyTarget();
                    t.assemblyId = rs.getInt(1);
                    t.accession = rs.getString(2);
                    t.version = rs.getString(3);
                    t.accessionType = rs.getString(4);
                    t.repliconType = rs.getString(5);
                    t.sampleId = rs.getObject(6) == null ? null : rs.getInt(6);
                    t.legacyStrainName = rs.getString(7);
                    t.legacyName = rs.getString(8);
                    t.legacySpecies = rs.getString(9);
                    t.legacyPathovar = rs.getString(10);
                    if (t.accession == null) {
                        continue;
                    }
                    targets.computeIfAbsent(t.accession.trim(), k -> new ArrayList<>()).add(t);
                }
            }
        }
        return targets;
    }

    private static Map<String, String> loadGenbankRecords(List<String> accessions, File cacheDir, int batchSize)
          throws Exception {
        Map<String, String> records = new HashMap<>();
        List<String> toFetch = new ArrayList<>();
        for (String acc : accessions) {
            String cached = readCache(cacheDir, "genbank", acc);
            if (cached != null) {
                records.put(acc, cached);
            } else {
                toFetch.add(acc);
            }
        }
        for (int i = 0; i < toFetch.size(); i += batchSize) {
            int end = Math.min(toFetch.size(), i + batchSize);
            List<String> batch = toFetch.subList(i, end);
            String gb = fetchGenbankBatch(batch);
            if (gb == null) {
                continue;
            }
            Map<String, String> parsed = splitGenbankRecords(gb);
            for (Map.Entry<String, String> entry : parsed.entrySet()) {
                String acc = entry.getKey();
                String record = entry.getValue();
                if (record != null) {
                    records.put(acc, record);
                    writeCache(cacheDir, "genbank", acc, record);
                }
            }
            sleepDelay();
        }
        return records;
    }

    private static BatchInputs buildBatchInputs(List<String> batch) {
        List<String> nuccore = new ArrayList<>();
        List<String> assembly = new ArrayList<>();
        for (String acc : batch) {
            if (isAssemblyAccession(acc)) {
                assembly.add(acc);
            } else {
                nuccore.add(acc);
            }
        }
        return new BatchInputs(nuccore, assembly);
    }

    private static Map<String, GenbankData> loadParsedRecords(BatchInputs inputs,
          Map<String, List<AssemblyTarget>> targets, File cacheDir, int batchSize) throws Exception {
        // fetch + parse nuccore and assembly in one map keyed by base accession
        Map<String, GenbankData> parsed = new HashMap<>();
        List<String> nuccoreFetchIds = buildNuccoreFetchIds(inputs.nuccore, targets);
        Map<String, String> records = loadGenbankRecords(nuccoreFetchIds, cacheDir, batchSize);
        for (String acc : inputs.nuccore) {
            String record = records.get(acc);
            if (record == null) {
                continue;
            }
            parsed.put(acc, parseGenbankRecord(record));
        }
        parsed.putAll(loadAssemblyRecords(inputs.assembly, cacheDir));
        return parsed;
    }

    private static Map<String, String> loadBatchBiosamples(Map<String, GenbankData> parsedRecords,
          File cacheDir, int batchSize, int batchIndex, int totalBatches) throws Exception {
        // only fetch biosample xml we have ids for
        Set<String> biosampleIds = new LinkedHashSet<>();
        for (GenbankData gb : parsedRecords.values()) {
            if (!isBlank(gb == null ? null : gb.biosampleId)) {
                biosampleIds.add(gb.biosampleId);
            }
        }
        if (!biosampleIds.isEmpty()) {
            System.out.println("NCBI: batch " + batchIndex + "/" + totalBatches
                  + " biosamples=" + biosampleIds.size());
        }
        return loadBiosampleRecords(new ArrayList<>(biosampleIds), cacheDir, batchSize);
    }

    private static int processBatch(Connection conn, File cacheDir, List<String> batch,
          Map<String, GenbankData> parsedRecords, Map<String, String> biosampleXml,
          Map<String, List<AssemblyTarget>> targets) throws Exception {
        int processed = 0;
        for (String acc : batch) {
            GenbankData gb = parsedRecords.get(acc);
            if (gb == null) {
                System.out.println("NCBI: missing genbank record accession=" + acc);
                continue;
            }
            System.out.println("NCBI: processing accession=" + acc);
            List<AssemblyTarget> list = targets.get(acc);
            if (list == null) {
                continue;
            }
            for (AssemblyTarget target : list) {
                if (!versionMatches(target.version, gb.version)) {
                    System.out.println("NCBI: version mismatch accession=" + acc
                          + " db_version=" + safe(target.version)
                          + " gb_version=" + safe(gb.version));
                    continue;
                }
                updateAssembly(conn, cacheDir, target, gb);
                String biosample = gb.biosampleId;
                if (!isBlank(biosample)) {
                    setSampleBiosampleId(conn, target, biosample);
                    String xml = biosampleXml.get(biosample);
                    BiosampleData bs = parseBiosampleXml(biosample, xml);
                    if (bs != null) {
                        updateSampleFromBiosample(conn, target, bs);
                    }
                } else {
                    System.out.println("NCBI: no biosample accession=" + acc);
                    updateSampleFromSource(conn, target, gb);
                }
                processed++;
            }
        }
        return processed;
    }

    private static List<String> buildNuccoreFetchIds(List<String> accessions,
          Map<String, List<AssemblyTarget>> targets) {
        Set<String> ids = new LinkedHashSet<>();
        for (String acc : accessions) {
            List<AssemblyTarget> list = targets.get(acc);
            if (list == null || list.isEmpty()) {
                ids.add(acc);
                continue;
            }
            boolean addedVersion = false;
            for (AssemblyTarget target : list) {
                if (!isBlank(target.version)) {
                    ids.add(acc + "." + target.version.trim());
                    addedVersion = true;
                }
            }
            if (!addedVersion) {
                ids.add(acc);
            }
        }
        return new ArrayList<>(ids);
    }

    private static Map<String, GenbankData> loadAssemblyRecords(List<String> accessions, File cacheDir)
          throws Exception {
        Map<String, GenbankData> records = new HashMap<>();
        for (String acc : accessions) {
            String cached = readCache(cacheDir, "assembly", acc);
            String xml = cached;
            if (xml == null) {
                xml = fetchAssemblySummaryXml(acc);
                if (xml != null) {
                    writeCache(cacheDir, "assembly", acc, xml);
                }
                sleepDelay();
            }
            if (xml == null) {
                continue;
            }
            AssemblySummary summary = parseAssemblySummary(xml);
            if (summary == null) {
                continue;
            }
            GenbankData gb = new GenbankData();
            gb.accession = summary.accession == null ? acc : summary.accession;
            gb.version = extractAccessionVersion(gb.accession);
            gb.definition = null;
            gb.assemblyLevel = summary.assemblyLevel;
            gb.organism = summary.organism;
            gb.taxonId = summary.taxonId;
            gb.biosampleId = summary.biosampleId;
            gb.fullRecord = xml;
            records.put(acc, gb);
        }
        return records;
    }

    private static Map<String, String> loadBiosampleRecords(List<String> biosamples, File cacheDir, int batchSize)
          throws Exception {
        Map<String, String> records = new HashMap<>();
        List<String> toFetch = new ArrayList<>();
        for (String id : biosamples) {
            String cached = readCache(cacheDir, "biosample", id);
            if (cached != null) {
                records.put(id, cached);
            } else {
                toFetch.add(id);
            }
        }
        for (int i = 0; i < toFetch.size(); i += batchSize) {
            int end = Math.min(toFetch.size(), i + batchSize);
            List<String> batch = toFetch.subList(i, end);
            String xml = fetchBiosampleBatch(batch);
            if (xml == null) {
                continue;
            }
            Map<String, String> parsed = splitBiosampleRecords(xml);
            for (Map.Entry<String, String> entry : parsed.entrySet()) {
                String id = entry.getKey();
                String record = entry.getValue();
                if (record != null) {
                    records.put(id, record);
                    writeCache(cacheDir, "biosample", id, record);
                }
            }
            sleepDelay();
        }
        return records;
    }

    private static String fetchGenbankBatch(List<String> accessions) throws Exception {
        // nuccore genbank flatfile
        String joined = String.join(",", accessions);
        String url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nuccore&id="
              + URLEncoder.encode(joined, "UTF-8") + "&rettype=gb&retmode=text";
        return fetchUrl(url);
    }

    private static String fetchBiosampleBatch(List<String> biosamples) throws Exception {
        // biosample xml
        String joined = String.join(",", biosamples);
        String url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=biosample&id="
              + URLEncoder.encode(joined, "UTF-8") + "&retmode=xml";
        return fetchUrl(url);
    }


    private static String fetchAssemblySummaryXml(String accession) throws Exception {
        // assembly esearch -> esummary
        String searchUrl = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=assembly&term="
              + URLEncoder.encode(accession, "UTF-8") + "&retmode=xml";
        String searchXml = fetchUrl(searchUrl);
        if (searchXml == null) {
            return null;
        }
        String id = extractXmlTag(searchXml, "Id");
        if (id == null || id.trim().isEmpty()) {
            return null;
        }
        String summaryUrl = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=assembly&id="
              + URLEncoder.encode(id.trim(), "UTF-8") + "&retmode=xml";
        return fetchUrl(summaryUrl);
    }

    private static String fetchUrl(String urlStr) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
        conn.setConnectTimeout(TIMEOUT_MS);
        conn.setReadTimeout(TIMEOUT_MS);
        conn.setRequestProperty("User-Agent", USER_AGENT);
        int code = conn.getResponseCode();
        if (code != 200) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(
              new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append('\n');
            }
        }
        return sb.toString();
    }

    private static void sleepDelay() {
        int delay = DEFAULT_DELAY_MS;
        String env = System.getenv("ANNOTTALE_NCBI_DELAY_MS");
        if (env != null) {
            try {
                delay = Integer.parseInt(env.trim());
            } catch (NumberFormatException ignored) {
                delay = DEFAULT_DELAY_MS;
            }
        }
        if (delay <= 0) {
            return;
        }
        try {
            Thread.sleep(delay);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private static Map<String, String> splitGenbankRecords(String gb) {
        Map<String, String> records = new HashMap<>();
        String[] parts = gb.split("\n//");
        for (String part : parts) {
            String record = part.trim();
            if (record.isEmpty()) {
                continue;
            }
            String accession = extractAccession(record);
            if (accession != null) {
                records.put(accession, record + "\n//\n");
            }
        }
        return records;
    }

    private static Map<String, String> splitBiosampleRecords(String xml) {
        Map<String, String> records = new HashMap<>();
        String[] parts = xml.split("</BioSample>");
        for (String part : parts) {
            String record = part.trim();
            if (record.isEmpty()) {
                continue;
            }
            String id = extractXmlId(record, "BioSample");
            if (id != null) {
                records.put(id, record + "</BioSample>");
            }
        }
        return records;
    }

    private static String extractAccession(String record) {
        for (String line : record.split("\n")) {
            if (line.startsWith("ACCESSION")) {
                String[] tokens = line.trim().split("\\s+");
                if (tokens.length >= 2) {
                    return tokens[1].trim();
                }
            }
        }
        return null;
    }

    private static String extractXmlId(String xml, String tag) {
        String key = "<" + tag + " ";
        int idx = xml.indexOf(key);
        if (idx < 0) {
            return null;
        }
        int acc = xml.indexOf("accession=\"", idx);
        if (acc < 0) {
            return null;
        }
        int start = acc + "accession=\"".length();
        int end = xml.indexOf("\"", start);
        if (end > start) {
            return xml.substring(start, end);
        }
        return null;
    }

    private static GenbankData parseGenbankRecord(String record) {
        GenbankData data = new GenbankData();
        data.accession = extractAccession(record);
        data.version = extractVersion(record);
        data.definition = extractBlockValue(record, "DEFINITION");
        data.fullRecord = record;
        data.organism = extractQualifier(record, "organism", null);
        data.taxonId = extractQualifier(record, "db_xref", "taxon");
        data.biosampleId = extractQualifier(record, "db_xref", "BioSample");
        if (isBlank(data.biosampleId)) {
            data.biosampleId = extractBioSampleId(record);
        }
        data.strain = extractQualifier(record, "strain", null);
        data.geo = extractQualifier(record, "geo_loc_name", null);
        data.collectionDate = extractQualifier(record, "collection_date", null);
        return data;
    }

    private static String extractVersion(String record) {
        for (String line : record.split("\n")) {
            if (line.startsWith("VERSION")) {
                String[] tokens = line.trim().split("\\s+");
                if (tokens.length >= 2) {
                    String[] parts = tokens[1].split("\\.");
                    if (parts.length > 1) {
                        return parts[1];
                    }
                }
            }
        }
        return null;
    }

    private static String extractAccessionVersion(String accession) {
        if (accession == null) {
            return null;
        }
        String trimmed = accession.trim();
        int dot = trimmed.lastIndexOf('.');
        if (dot > 0 && dot < trimmed.length() - 1) {
            String ver = trimmed.substring(dot + 1);
            if (ver.matches("\\d+")) {
                return ver;
            }
        }
        return null;
    }

    private static String extractBlockValue(String gb, String label) {
        String marker = label + "  ";
        int idx = gb.indexOf(marker);
        if (idx < 0) {
            return null;
        }
        int start = idx + marker.length();
        StringBuilder sb = new StringBuilder();
        int pos = start;
        while (pos < gb.length()) {
            int lineEnd = gb.indexOf('\n', pos);
            if (lineEnd < 0) {
                lineEnd = gb.length();
            }
            String line = gb.substring(pos, lineEnd);
            if (!line.startsWith("            ")) {
                if (pos != start) {
                    break;
                }
            }
            sb.append(line.trim()).append(' ');
            pos = lineEnd + 1;
        }
        String val = sb.toString().trim();
        return val.isEmpty() ? null : val;
    }

    private static String extractQualifier(String gb, String qualifier, String prefix) {
        String key = "/" + qualifier + "=\"";
        int idx = gb.indexOf(key);
        while (idx >= 0) {
            int start = idx + key.length();
            int end = gb.indexOf("\"", start);
            if (end < 0) {
                break;
            }
            if (end > start) {
                String val = gb.substring(start, end);
                if (prefix == null) {
                    return val;
                }
                String pfx = prefix + ":";
                if (val.startsWith(pfx)) {
                    return val.substring(pfx.length());
                }
            }
            idx = gb.indexOf(key, end + 1);
        }
        return null;
    }

    private static String extractBioSampleId(String gb) {
        int idx = gb.indexOf("BioSample:");
        if (idx >= 0) {
            int start = idx + "BioSample:".length();
            int end = gb.indexOf('\n', start);
            String raw = end > start ? gb.substring(start, end).trim() : gb.substring(start).trim();
            return raw.isEmpty() ? null : raw;
        }
        return null;
    }

    private static BiosampleData parseBiosampleXml(String biosampleId, String xml) {
        if (isBlank(xml)) {
            return null;
        }
        BiosampleData data = new BiosampleData();
        data.biosampleId = biosampleId;
        data.strain = extractXmlAttribute(xml, "strain");
        data.geo = extractXmlAttribute(xml, "geo_loc_name");
        data.collectionDate = extractXmlAttribute(xml, "collection_date");
        return data;
    }

    private static String extractXmlAttribute(String xml, String attributeName) {
        String needle = "attribute_name=\"" + attributeName + "\"";
        int idx = xml.indexOf(needle);
        if (idx < 0) {
            return null;
        }
        int valStart = xml.indexOf('>', idx);
        int valEnd = xml.indexOf("</Attribute>", valStart);
        if (valStart < 0 || valEnd < 0) {
            return null;
        }
        return trimToNull(xml.substring(valStart + 1, valEnd));
    }

    private static void updateAssembly(Connection conn, File cacheDir, AssemblyTarget target, GenbankData gb)
          throws Exception {
        if (isBlank(target.accessionType)) {
            String inferred = inferAccessionType(target.accession);
            if (inferred != null) {
                try (PreparedStatement ps = conn.prepareStatement(
                      "UPDATE assembly SET accession_type=? WHERE id=?")) {
                    ps.setString(1, inferred);
                    ps.setInt(2, target.assemblyId);
                    ps.executeUpdate();
                }
                target.accessionType = inferred;
            }
        }
        if (isBlank(target.repliconType)) {
            String type = detectRepliconType(gb, target.accessionType);
            if (type != null) {
                try (PreparedStatement ps = conn.prepareStatement(
                      "UPDATE assembly SET replicon_type=? WHERE id=?")) {
                    ps.setString(1, type);
                    ps.setInt(2, target.assemblyId);
                    ps.executeUpdate();
                }
            }
        }
        if (!isBlank(gb.taxonId)) {
            TaxonParts parts = parseTaxonFromName(gb.organism);
            String rank = deriveRank(parts);
            int ncbiId = getOrCreateNcbiTaxon(conn, gb, parts, rank);
            if (target.sampleId != null) {
                setSampleTaxon(conn, target.sampleId, ncbiId);
            }
            updateTaxonomyLineage(conn, cacheDir, gb.taxonId, ncbiId, rank);
            if (hasLegacyMismatch(target, gb, parts)) {
                System.out.println("NCBI: taxonomy mismatch sample_id=" + target.sampleId
                      + " legacy_name=" + safe(target.legacyName)
                      + " ncbi_name=" + safe(gb.organism));
            }
        }
    }

    private static String detectRepliconType(GenbankData gb, String accessionType) {
        if ("assembly".equalsIgnoreCase(accessionType)) {
            String fromLevel = detectFromAssemblyLevel(gb == null ? null : gb.assemblyLevel);
            if (fromLevel != null) {
                return fromLevel;
            }
            return "genome";
        }
        String def = gb == null || gb.definition == null ? null : gb.definition.toLowerCase();
        if (def != null) {
            if (def.contains("chromosome")) {
                return "chromosome";
            }
            if (def.contains("plasmid")) {
                return "plasmid";
            }
            if (def.contains("genome")) {
                return "genome";
            }
            if (def.contains("gene")) {
                return "gene";
            }
        }
        String full = gb == null || gb.fullRecord == null ? null : gb.fullRecord.toLowerCase();
        if (full != null) {
            if (full.contains("chromosome")) {
                return "chromosome";
            }
            if (full.contains("plasmid")) {
                return "plasmid";
            }
            if (full.contains("genome")) {
                return "genome";
            }
            if (full.contains("gene")) {
                return "gene";
            }
        }
        return null;
    }

    private static String detectFromAssemblyLevel(String assemblyLevel) {
        if (isBlank(assemblyLevel)) {
            return null;
        }
        String lower = assemblyLevel.toLowerCase();
        if (lower.contains("chromosome")) {
            return "chromosome";
        }
        if (lower.contains("plasmid")) {
            return "plasmid";
        }
        if (lower.contains("genome") || lower.contains("complete") || lower.contains("scaffold")
              || lower.contains("contig")) {
            return "genome";
        }
        return null;
    }

    private static String inferAccessionType(String accession) {
        if (accession == null) {
            return null;
        }
        String trimmed = accession.trim().toUpperCase();
        if (trimmed.startsWith("GCA_") || trimmed.startsWith("GCF_")) {
            return "assembly";
        }
        return "nuccore";
    }

    private static boolean isAssemblyAccession(String accession) {
        return "assembly".equalsIgnoreCase(inferAccessionType(accession));
    }

    private static void updateSampleFromBiosample(Connection conn, AssemblyTarget target, BiosampleData bs)
          throws Exception {
        if (target.sampleId == null) {
            return;
        }
        String strainValue = normalizeStrainRaw(bs.strain);
        if (!strainMatches(target.legacyStrainName, bs.strain)) {
            System.out.println("NCBI: strain mismatch sample_id=" + target.sampleId
                  + " db_strain=" + safe(target.legacyStrainName)
                  + " ncbi_biosample_strain=" + safe(bs.strain));
        } else if (shouldReplaceStrain(target.legacyStrainName, bs.strain)) {
            if (!safe(strainValue).equals(safe(target.legacyStrainName))) {
                System.out.println("NCBI: strain replace sample_id=" + target.sampleId
                      + " before=" + safe(target.legacyStrainName)
                      + " after=" + safe(strainValue)
                      + " source=biosample");
            }
        }
        String biosampleToSet = bs.biosampleId;
        if (biosampleToSet != null) {
            Integer existing = findSampleIdByBiosample(conn, biosampleToSet);
            if (existing != null && !existing.equals(target.sampleId)) {
                System.out.println("NCBI: biosample already linked biosample_id=" + biosampleToSet
                      + " existing_sample_id=" + existing + " target_sample_id=" + target.sampleId);
                biosampleToSet = null;
            }
        }
        String geo = sanitizeGeo(bs.geo);
        String date = sanitizeCollectionDate(bs.collectionDate);
        try (PreparedStatement ps = conn.prepareStatement(
              "UPDATE samples SET "
                    + "strain_name=COALESCE(strain_name, ?), "
                    + "geo_tag=COALESCE(geo_tag, ?), "
                    + "collection_date=COALESCE(collection_date, ?), "
                    + "biosample_id=COALESCE(biosample_id, ?) "
                    + "WHERE id=?")) {
            ps.setString(1, strainValue);
            ps.setString(2, geo);
            ps.setString(3, date);
            ps.setString(4, biosampleToSet);
            ps.setInt(5, target.sampleId);
            ps.executeUpdate();
        }
    }

    private static void setSampleBiosampleId(Connection conn, AssemblyTarget target, String biosampleId)
          throws Exception {
        if (target.sampleId == null || isBlank(biosampleId)) {
            return;
        }
        Integer existing = findSampleIdByBiosample(conn, biosampleId);
        if (existing != null && !existing.equals(target.sampleId)) {
            System.out.println("NCBI: biosample already linked biosample_id=" + biosampleId
                  + " existing_sample_id=" + existing + " target_sample_id=" + target.sampleId);
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement(
              "UPDATE samples SET biosample_id=COALESCE(biosample_id, ?) WHERE id=?")) {
            ps.setString(1, biosampleId);
            ps.setInt(2, target.sampleId);
            ps.executeUpdate();
        }
    }
    private static void updateSampleFromSource(Connection conn, AssemblyTarget target, GenbankData gb)
          throws Exception {
        if (target.sampleId == null) {
            return;
        }
        String strainValue = normalizeStrainRaw(gb.strain);
        if (!strainMatches(target.legacyStrainName, gb.strain)) {
            System.out.println("NCBI: strain mismatch sample_id=" + target.sampleId
                  + " db_strain=" + safe(target.legacyStrainName)
                  + " ncbi_source_strain=" + safe(gb.strain));
        } else if (shouldReplaceStrain(target.legacyStrainName, gb.strain)) {
            if (!safe(strainValue).equals(safe(target.legacyStrainName))) {
                System.out.println("NCBI: strain replace sample_id=" + target.sampleId
                      + " before=" + safe(target.legacyStrainName)
                      + " after=" + safe(strainValue)
                      + " source=genbank");
            }
        }
        String geo = sanitizeGeo(gb.geo);
        String date = sanitizeCollectionDate(gb.collectionDate);
        try (PreparedStatement ps = conn.prepareStatement(
              "UPDATE samples SET "
                    + "strain_name=COALESCE(strain_name, ?), "
                    + "geo_tag=COALESCE(geo_tag, ?), "
                    + "collection_date=COALESCE(collection_date, ?) "
                    + "WHERE id=?")) {
            ps.setString(1, strainValue);
            ps.setString(2, geo);
            ps.setString(3, date);
            ps.setInt(4, target.sampleId);
            ps.executeUpdate();
        }
    }

    private static boolean versionMatches(String target, String record) {
        if (isBlank(target) || isBlank(record)) {
            return true;
        }
        return target.equals(record);
    }

    private static boolean strainMatches(String sampleStrain, String refStrain) {
        if (isBlank(refStrain)) {
            return true;
        }
        if (isBlank(sampleStrain)) {
            return true;
        }
        String normSample = normalizeStrainToken(sampleStrain);
        if (isPlaceholderStrain(normSample)) {
            return true;
        }
        String normRef = normalizeStrainToken(refStrain);
        if (normRef == null || isPlaceholderStrain(normRef)) {
            return true;
        }
        if (normRef.equalsIgnoreCase(normSample)) {
            return true;
        }
        String cleanedRef = normalizeStrainKey(refStrain);
        String cleanedSample = normalizeStrainKey(sampleStrain);
        if (cleanedRef == null || cleanedSample == null) {
            return false;
        }
        return cleanedRef.equalsIgnoreCase(cleanedSample);
    }

    private static boolean shouldReplaceStrain(String sampleStrain, String refStrain) {
        if (isBlank(refStrain)) {
            return false;
        }
        if (isBlank(sampleStrain)) {
            return true;
        }
        String normSample = normalizeStrainToken(sampleStrain);
        if (isPlaceholderStrain(normSample)) {
            return true;
        }
        String cleanedRef = normalizeStrainKey(refStrain);
        String cleanedSample = normalizeStrainKey(sampleStrain);
        if (cleanedRef == null || cleanedSample == null) {
            return false;
        }
        return cleanedRef.equalsIgnoreCase(cleanedSample);
    }

    private static Integer findNcbiTaxonomyId(Connection conn, String taxonId) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT id FROM taxonomy WHERE ncbi_tax_id=?")) {
            ps.setInt(1, Integer.parseInt(taxonId));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return null;
    }

    private static Integer findSampleIdByBiosample(Connection conn, String biosampleId) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT id FROM samples WHERE biosample_id=?")) {
            ps.setString(1, biosampleId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return null;
    }

    private static int getOrCreateNcbiTaxon(Connection conn, GenbankData gb, TaxonParts parts, String rank)
          throws Exception {
        Integer existing = findNcbiTaxonomyId(conn, gb.taxonId);
        if (existing != null) {
            try (PreparedStatement ps = conn.prepareStatement(
                  "UPDATE taxonomy SET raw_name=?, species=?, pathovar=?, rank=? WHERE id=?")) {
                ps.setString(1, gb.organism);
                ps.setObject(2, parts == null ? null : parts.species);
                ps.setObject(3, parts == null ? null : parts.pathovar);
                ps.setObject(4, rank);
                ps.setInt(5, existing);
                ps.executeUpdate();
            }
            return existing;
        }
        try (PreparedStatement ps = conn.prepareStatement(
              "INSERT INTO taxonomy(ncbi_tax_id, raw_name, species, pathovar, rank) "
                    + "VALUES (?,?,?,?,?)",
              PreparedStatement.RETURN_GENERATED_KEYS)) {
            ps.setInt(1, Integer.parseInt(gb.taxonId));
            ps.setString(2, gb.organism);
            ps.setObject(3, parts == null ? null : parts.species);
            ps.setObject(4, parts == null ? null : parts.pathovar);
            ps.setObject(5, rank);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        throw new IllegalStateException("Failed to create taxonomy for taxon " + gb.taxonId);
    }

    private static void setSampleTaxon(Connection conn, int sampleId, int taxonId) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
              "UPDATE samples SET taxon_id=? WHERE id=?")) {
            ps.setInt(1, taxonId);
            ps.setInt(2, sampleId);
            ps.executeUpdate();
        }
    }

    private static void updateTaxonomyLineage(Connection conn, File cacheDir, String taxonId, int rowId,
          String derivedRank) throws Exception {
        TaxonomyRecord record = fetchTaxonomyRecord(cacheDir, taxonId);
        if (record == null) {
            return;
        }
        Integer parentRow = null;
        if (record.parentTaxId != null) {
            TaxonomyRecord parent = fetchTaxonomyRecord(cacheDir, record.parentTaxId);
            if (parent != null) {
                parentRow = getOrCreateNcbiTaxon(conn, parent);
            }
        }
        String rankToSet = normalizeRank(record.rank, derivedRank);
        if ("pathogroup".equalsIgnoreCase(rankToSet)) {
            TaxonParts parts = parseTaxonFromName(record.scientificName);
            Integer speciesRow = findSpeciesTaxon(conn, parts == null ? null : parts.species);
            if (speciesRow != null) {
                parentRow = speciesRow;
            }
        }
        try (PreparedStatement ps = conn.prepareStatement(
              "UPDATE taxonomy SET rank=?, parent_id=? WHERE id=?")) {
            ps.setString(1, rankToSet);
            if (parentRow == null) {
                ps.setObject(2, null);
            } else {
                ps.setInt(2, parentRow);
            }
            ps.setInt(3, rowId);
            ps.executeUpdate();
        }
    }

    private static Integer findSpeciesTaxon(Connection conn, String species) throws Exception {
        if (isBlank(species)) {
            return null;
        }
        try (PreparedStatement ps = conn.prepareStatement(
              "SELECT id FROM taxonomy WHERE rank='species' AND "
                    + "(raw_name=? OR species=?) LIMIT 1")) {
            ps.setString(1, species);
            ps.setString(2, species);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return null;
    }

    private static TaxonomyRecord fetchTaxonomyRecord(File cacheDir, String taxonId) throws Exception {
        String cached = readCache(cacheDir, "taxonomy", taxonId);
        if (cached == null) {
            String xml = fetchTaxonomy(taxonId);
            if (xml == null) {
                return null;
            }
            writeCache(cacheDir, "taxonomy", taxonId, xml);
            cached = xml;
        }
        return parseTaxonomyXml(cached, taxonId);
    }

    private static String fetchTaxonomy(String taxonId) throws Exception {
        String url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=taxonomy&id="
              + URLEncoder.encode(taxonId, "UTF-8") + "&retmode=xml";
        return fetchUrl(url);
    }

    private static TaxonomyRecord parseTaxonomyXml(String xml, String taxonId) {
        if (xml == null || xml.trim().isEmpty()) {
            return null;
        }
        TaxonomyRecord record = new TaxonomyRecord();
        record.taxonId = taxonId;
        record.scientificName = extractXmlTag(xml, "ScientificName");
        record.rank = extractXmlTag(xml, "Rank");
        record.parentTaxId = extractXmlTag(xml, "ParentTaxId");
        return record;
    }

    private static int getOrCreateNcbiTaxon(Connection conn, TaxonomyRecord record) throws Exception {
        Integer existing = findNcbiTaxonomyId(conn, record.taxonId);
        TaxonParts parts = parseTaxonFromName(record.scientificName);
        String rank = normalizeRank(record.rank, deriveRank(parts));
        if (existing != null) {
            try (PreparedStatement ps = conn.prepareStatement(
                  "UPDATE taxonomy SET raw_name=?, species=?, pathovar=?, rank=? WHERE id=?")) {
                ps.setString(1, record.scientificName);
                ps.setObject(2, parts == null ? null : parts.species);
                ps.setObject(3, parts == null ? null : parts.pathovar);
                ps.setObject(4, rank);
                ps.setInt(5, existing);
                ps.executeUpdate();
            }
            return existing;
        }
        try (PreparedStatement ps = conn.prepareStatement(
              "INSERT INTO taxonomy(ncbi_tax_id, raw_name, species, pathovar, rank) "
                    + "VALUES (?,?,?,?,?)",
              PreparedStatement.RETURN_GENERATED_KEYS)) {
            ps.setInt(1, Integer.parseInt(record.taxonId));
            ps.setString(2, record.scientificName);
            ps.setObject(3, parts == null ? null : parts.species);
            ps.setObject(4, parts == null ? null : parts.pathovar);
            ps.setObject(5, rank);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        throw new IllegalStateException("Failed to create taxonomy for taxon " + record.taxonId);
    }

    private static String extractXmlTag(String xml, String tag) {
        String open = "<" + tag + ">";
        String close = "</" + tag + ">";
        int start = xml.indexOf(open);
        if (start < 0) {
            return null;
        }
        int end = xml.indexOf(close, start + open.length());
        if (end < 0) {
            return null;
        }
        return trimToNull(xml.substring(start + open.length(), end));
    }

    private static AssemblySummary parseAssemblySummary(String xml) {
        if (xml == null || xml.trim().isEmpty()) {
            return null;
        }
        AssemblySummary summary = new AssemblySummary();
        summary.accession = extractEsummaryItem(xml, "AssemblyAccession");
        summary.organism = firstNonNull(
              extractEsummaryItem(xml, "SpeciesName"),
              extractEsummaryItem(xml, "Organism"));
        summary.taxonId = extractEsummaryItem(xml, "Taxid");
        summary.biosampleId = firstNonNull(
              extractEsummaryItem(xml, "BioSampleAccn"),
              extractEsummaryItem(xml, "BioSampleAccession"),
              extractEsummaryItem(xml, "BioSample"));
        summary.assemblyLevel = extractEsummaryItem(xml, "AssemblyLevel");
        return summary;
    }

    private static String extractEsummaryItem(String xml, String name) {
        String needle = "Name=\"" + name + "\"";
        int idx = xml.indexOf(needle);
        if (idx < 0) {
            return null;
        }
        int valStart = xml.indexOf('>', idx);
        int valEnd = xml.indexOf("</Item>", valStart);
        if (valStart < 0 || valEnd < 0) {
            return null;
        }
        return trimToNull(xml.substring(valStart + 1, valEnd));
    }

    private static String firstNonNull(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.trim().isEmpty()) {
                return value;
            }
        }
        return null;
    }

    private static boolean hasLegacyMismatch(AssemblyTarget target, GenbankData gb, TaxonParts parts) {
        if (target.legacyName == null && target.legacySpecies == null && target.legacyPathovar == null) {
            return false;
        }
        String ncbiName = gb.organism == null ? null : gb.organism.trim();
        String legacyName = target.legacyName == null ? null : target.legacyName.trim();
        if (ncbiName != null && legacyName != null && !ncbiName.equalsIgnoreCase(legacyName)) {
            return true;
        }
        if (parts != null) {
            if (target.legacySpecies != null && !target.legacySpecies.equalsIgnoreCase(parts.species)) {
                return true;
            }
            if (target.legacyPathovar != null && parts.pathovar != null
                  && !target.legacyPathovar.equalsIgnoreCase(parts.pathovar)) {
                return true;
            }
        }
        return false;
    }

    private static String normalizeStrainToken(String raw) {
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

    private static String normalizeStrainRaw(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String normalizeStrainKey(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        String cleaned = trimmed.replaceAll("[^A-Za-z0-9]", "");
        if (cleaned.isEmpty()) {
            return null;
        }
        return cleaned;
    }

    private static boolean isPlaceholderStrain(String value) {
        if (value == null) {
            return true;
        }
        String v = value.trim().toLowerCase();
        return v.isEmpty() || v.startsWith("unnamed") || v.equals("x");
    }

    private static String safe(String val) {
        return val == null ? "null" : val;
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String sanitizeGeo(String value) {
        String trimmed = trimToNull(value);
        if (trimmed == null) {
            return null;
        }
        if (isPlaceholderToken(trimmed.toLowerCase())) {
            return null;
        }
        return trimmed;
    }

    private static String sanitizeCollectionDate(String value) {
        String trimmed = trimToNull(value);
        if (trimmed == null) {
            return null;
        }
        if (isPlaceholderToken(trimmed.toLowerCase())) {
            return null;
        }
        return trimmed;
    }

    private static boolean isPlaceholderToken(String lower) {
        return lower.equals("-") || lower.equals("unknown") || lower.equals("missing")
              || lower.equals("na") || lower.equals("n/a") || lower.equals("not applicable")
              || lower.equals("not collected");
    }


    private static TaxonParts parseTaxonFromName(String organism) {
        if (isBlank(organism)) {
            return null;
        }
        String[] tokens = organism.trim().split("\\s+");
        if (tokens.length < 2) {
            return null;
        }
        String genus = tokens[0];
        String species = genus + " " + tokens[1].toLowerCase();
        String pathovar = null;
        int pvIndex = -1;
        for (int i = 0; i < tokens.length; i++) {
            String t = tokens[i].toLowerCase();
            if (t.equals("pv") || t.equals("pv.")) {
                pvIndex = i;
                break;
            }
        }
        boolean hasSuffix = false;
        if (pvIndex >= 0 && pvIndex + 1 < tokens.length) {
            pathovar = tokens[pvIndex + 1].toLowerCase();
            if (pvIndex + 2 < tokens.length) {
                hasSuffix = true;
            }
        }
        String canonical = species;
        if (pathovar != null && !pathovar.isEmpty()) {
            canonical = species + " pv. " + pathovar;
        }
        return new TaxonParts(species, pathovar, canonical, hasSuffix);
    }

    private static String deriveRank(TaxonParts parts) {
        if (parts == null) {
            return null;
        }
        if (parts.hasSuffix) {
            return "strain";
        }
        if (parts.pathovar != null && !parts.pathovar.isEmpty()) {
            return "pathovar";
        }
        if (parts.species != null && !parts.species.isEmpty()) {
            String speciesLower = parts.species.toLowerCase();
            if (speciesLower.endsWith(" sp.") || speciesLower.endsWith(" sp")) {
                return "genus";
            }
            return "species";
        }
        return null;
    }

    private static String normalizeRank(String ncbiRank, String fallback) {
        if (isBlank(ncbiRank)) {
            return fallback;
        }
        String trimmed = ncbiRank.trim();
        if (trimmed.equalsIgnoreCase("no rank")) {
            return fallback;
        }
        return trimmed;
    }

    private static final class TaxonParts {
        final String species;
        final String pathovar;
        final String canonicalName;
        final boolean hasSuffix;

        private TaxonParts(String species, String pathovar, String canonicalName, boolean hasSuffix) {
            this.species = species;
            this.pathovar = pathovar;
            this.canonicalName = canonicalName;
            this.hasSuffix = hasSuffix;
        }
    }

    private static String readCache(File dir, String type, String key) throws Exception {
        File file = new File(dir, type + "_" + sanitize(key) + ".txt");
        if (!file.exists()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append('\n');
            }
        }
        String content = sb.toString().trim();
        return content.isEmpty() ? null : content;
    }

    private static void writeCache(File dir, String type, String key, String content) throws Exception {
        File file = new File(dir, type + "_" + sanitize(key) + ".txt");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            bw.write(content);
        }
    }

    private static String sanitize(String key) {
        return key.replaceAll("[^A-Za-z0-9_.-]", "_");
    }

    private static final class AssemblyTarget {
        int assemblyId;
        String accession;
        String version;
        String accessionType;
        String repliconType;
        Integer sampleId;
        String legacyStrainName;
        String legacyName;
        String legacySpecies;
        String legacyPathovar;
    }

    private static final class GenbankData {
        String accession;
        String version;
        String definition;
        String fullRecord;
        String assemblyLevel;
        String organism;
        String taxonId;
        String biosampleId;
        String strain;
        String geo;
        String collectionDate;
    }

    private static final class BiosampleData {
        String biosampleId;
        String strain;
        String geo;
        String collectionDate;
    }

    private static final class AssemblySummary {
        String accession;
        String organism;
        String taxonId;
        String biosampleId;
        String assemblyLevel;
    }

    private static final class BatchInputs {
        final List<String> nuccore;
        final List<String> assembly;

        private BatchInputs(List<String> nuccore, List<String> assembly) {
            this.nuccore = nuccore;
            this.assembly = assembly;
        }
    }

    private static final class TaxonomyRecord {
        String taxonId;
        String scientificName;
        String rank;
        String parentTaxId;
    }

}
