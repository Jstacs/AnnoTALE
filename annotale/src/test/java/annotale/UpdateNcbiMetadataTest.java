package annotale;

import annotale.storage.SQLiteSchema;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UpdateNcbiMetadataTest {

    @Test
    public void storesOnlyTheTaxonomyLayerName() {
        assertEquals("KACC 10331", UpdateNcbiMetadata.taxonomyLayerName(
              "Xanthomonas oryzae pv. oryzae str. KACC 10331", "Xanthomonas oryzae pv. oryzae", "strain"));
        assertEquals("Aw12879", UpdateNcbiMetadata.taxonomyLayerName(
              "Xanthomonas citri subsp. citri Aw12879", "Xanthomonas citri", "strain"));
        assertEquals("phaseoli var. fuscans", UpdateNcbiMetadata.taxonomyLayerName(
              "Xanthomonas citri pv. phaseoli var. fuscans", "Xanthomonas citri pv. phaseoli", "pathovar"));
        assertEquals("oryzae", UpdateNcbiMetadata.taxonomyLayerName(
              "Xanthomonas oryzae pv. oryzae", "Xanthomonas oryzae", "pathovar"));
        assertEquals("oryzae", UpdateNcbiMetadata.taxonomyLayerName(
              "Xanthomonas oryzae", "Xanthomonas", "species"));
        assertEquals("sp. NCPPB 2586", UpdateNcbiMetadata.taxonomyLayerName(
              "Xanthomonas sp. NCPPB 2586", "unclassified Xanthomonas", "species"));
        assertEquals("sp. NCPPB 3761", UpdateNcbiMetadata.taxonomyLayerName(
              "Xanthomonas sp. NCPPB 3761", "Xanthomonas", "species"));
        assertEquals("Xanthomonas", UpdateNcbiMetadata.taxonomyLayerName("Xanthomonas", null, "genus"));
    }

    @Test
    public void expandsTheLegacyGenusAbbreviationBeforeComparingTaxonomy() {
        assertEquals("Xanthomonas axonopodis pv. ricini", UpdateNcbiMetadata.canonicalizeTaxonName(
              "X axonopodis pv. ricini"));
    }

    @Test
    public void ignoresTheStrainAfterKnownLegacyTaxonomyCodes() {
        assertEquals("Xanthomonas oryzae pv. oryzae", UpdateNcbiMetadata.canonicalizeTaxonName("Xoo A57"));
        assertEquals("Xanthomonas translucens pv. undulosa", UpdateNcbiMetadata.canonicalizeTaxonName("Xtu ICMP11055"));
        assertEquals("Xanthomonas citri pv. phaseoli var. fuscans", UpdateNcbiMetadata.canonicalizeTaxonName("Xcp CFBP7767"));
        assertEquals("Xanthomonas citri pv. vignicola", UpdateNcbiMetadata.canonicalizeTaxonName("Xcv CFBP7112"));
        assertEquals("Xanthomonas citri pv. glycines", UpdateNcbiMetadata.canonicalizeTaxonName("Xcg 12-2"));
        assertEquals("Xanthomonas phaseoli pv. phaseoli", UpdateNcbiMetadata.canonicalizeTaxonName("Xpp CFBP6164"));
        assertEquals("Xanthomonas phaseoli pv. manihotis", UpdateNcbiMetadata.canonicalizeTaxonName("Xpm CHN01"));
        assertEquals("Xanthomonas axonopodis pv. vasculorum", UpdateNcbiMetadata.canonicalizeTaxonName("Xav NCPPB"));
        assertEquals("Xanthomonas axonopodis pv. commiphoreae", UpdateNcbiMetadata.canonicalizeTaxonName("Xac LMG26789"));
        assertEquals("Xanthomonas citri pv. fuscans", UpdateNcbiMetadata.canonicalizeTaxonName("Xcf PR8F"));
        assertEquals("Xanthomonas translucens pv. translucens", UpdateNcbiMetadata.canonicalizeTaxonName("Xtt UPB886"));
        assertEquals("Xanthomonas translucens", UpdateNcbiMetadata.canonicalizeTaxonName("Xt"));
        assertEquals("Xanthomonas translucens pv. secalis", UpdateNcbiMetadata.canonicalizeTaxonName("Xt secalis"));
        assertEquals("Xanthomonas translucens pv. hordei", UpdateNcbiMetadata.canonicalizeTaxonName("Xt pv. hordei UPB947"));
        assertEquals(true, UpdateNcbiMetadata.matchesLegacyTaxonomy("Xcc A306", "Xanthomonas citri subsp. citri 306"));
        assertEquals(true, UpdateNcbiMetadata.matchesLegacyTaxonomy("Xcp LMG 859", "Xanthomonas citri pv. punicae"));
    }

    @Test
    public void doesNotReplacePlaceholderStrainsWithTaxonomyNames() {
        assertEquals(false, UpdateNcbiMetadata.shouldReplaceStrain("X", "Xanthomonas"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNonPositiveBatchSizes() {
        UpdateNcbiMetadata.validateBatchSize(0);
    }

    @Test
    public void detectsBrokenTaxonomyReferencesAndCycles() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:");
             Statement st = conn.createStatement()) {
            SQLiteSchema.ensureSchema(conn);
            st.executeUpdate("INSERT INTO taxonomy(id,name,rank,parent_id) VALUES (1,'a','species',2)");
            st.executeUpdate("INSERT INTO taxonomy(id,name,rank,parent_id) VALUES (2,'b','species',1)");
            st.executeUpdate("INSERT INTO taxonomy(id,name,rank,parent_id) VALUES (3,'orphan','species',99)");
            st.executeUpdate("INSERT INTO samples(id,legacy_strain_name,taxon_id) VALUES (1,'sample',98)");
            List<String> issues = VerifyXmlSqlite.taxonomyIssues(conn);
            assertTrue(issues.stream().anyMatch(issue -> issue.contains("taxonomy cycle")));
            assertTrue(issues.stream().anyMatch(issue -> issue.contains("missing taxonomy parent")));
            assertTrue(issues.stream().anyMatch(issue -> issue.contains("missing sample taxon")));
        }
    }

    @Test
    public void readsTaggedAssemblySummaryValues() {
        assertEquals("SAMN02469928", UpdateNcbiMetadata.extractEsummaryItem(
              "<DocumentSummary><BioSampleAccn>SAMN02469928</BioSampleAccn></DocumentSummary>", "BioSampleAccn"));
    }

    @Test
    public void assignsUnambiguousLegacyPathovarsByLineage() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:");
             Statement st = conn.createStatement()) {
            SQLiteSchema.ensureSchema(conn);
            st.executeUpdate("INSERT INTO taxonomy(id,ncbi_tax_id,name,rank) VALUES (1,338,'Xanthomonas','genus')");
            st.executeUpdate("INSERT INTO taxonomy(id,ncbi_tax_id,name,rank,parent_id) VALUES (2,347,'oryzae','species',1)");
            st.executeUpdate("INSERT INTO taxonomy(id,ncbi_tax_id,name,rank,parent_id) VALUES (3,64187,'oryzae','pathovar',2)");
            st.executeUpdate("INSERT INTO taxonomy(id,ncbi_tax_id,name,rank,parent_id) VALUES (4,129394,'oryzicola','pathovar',2)");
            for (String name : new String[]{"Xoo arbitrary-strain", "Xoc arbitrary-strain"}) {
                st.executeUpdate("INSERT INTO samples(legacy_strain_name) VALUES ('" + name + "')");
            }
            UpdateNcbiMetadata.assignLegacyTaxa(conn);
            try (ResultSet rs = st.executeQuery(
                  "SELECT taxon_id, taxon_assignment_source FROM samples ORDER BY id")) {
                assertEquals(true, rs.next()); assertEquals(3, rs.getInt(1)); assertEquals("legacy-derived", rs.getString(2));
                assertEquals(true, rs.next()); assertEquals(4, rs.getInt(1)); assertEquals("legacy-derived", rs.getString(2));
            }
        }
    }

}
