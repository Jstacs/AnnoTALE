package annotale.storage;

import annotale.TALEFamilyBuilder;
import annotale.TALEConsensus;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class SqliteTALEFamilyLoaderTest {

    @Test
    public void preservesRvdOnlyTales() throws Exception {
        File db = File.createTempFile("annotale-rvd", ".db");
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + db.getAbsolutePath());
             Statement st = conn.createStatement()) {
            SQLiteSchema.ensureSchema(conn);
            st.executeUpdate("INSERT INTO analysis_config VALUES ('default','SEMI_GLOBAL',1,0.1,0.1,'AVERAGE',0.01,5,1,1,0.2,0.8,0)");
            st.executeUpdate("INSERT INTO dmat(id,data) VALUES (1,'0$')");
            st.executeUpdate("INSERT INTO samples(id,legacy_strain_name) VALUES (1,'sample')");
            st.executeUpdate("INSERT INTO assembly(id,sample_id) VALUES (1,1)");
            st.executeUpdate("INSERT INTO tale(id,legacy_name,is_new,assembly_id) VALUES (1,'Tal1',0,1)");
            st.executeUpdate("INSERT INTO repeat(tale_id,repeat_ordinal,rvd,rvd_len) VALUES (1,0,'NI',2)");
            st.executeUpdate("INSERT INTO dmat_tale_order(ordinal,tale_id) VALUES (0,1)");
            st.executeUpdate("INSERT INTO tale_family(name,member_count,tree_newick) VALUES ('1',1,'1;')");
            st.executeUpdate("INSERT INTO tale_family_member(family_id,tale_id) VALUES ('1',1)");
        }
        try {
            TALEFamilyBuilder builder = SqliteTALEFamilyLoader.load(db.getAbsolutePath());
            assertEquals("NI", builder.getFamily(0).getFamilyMembers()[0].getRvdSequence().toString());
        } finally {
            db.delete();
        }
    }

    @Test
    public void restoresStoredProteinAndDnaPartsWithoutGuessingBoundaries() throws Exception {
        File db = File.createTempFile("annotale-parts", ".db");
        String proteinRepeat = TALEConsensus.repeat.toString();
        String dnaRepeat = "GCT".repeat(proteinRepeat.length());
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + db.getAbsolutePath());
             Statement st = conn.createStatement()) {
            SQLiteSchema.ensureSchema(conn);
            st.executeUpdate("INSERT INTO analysis_config VALUES ('default','SEMI_GLOBAL',1,0.1,0.1,'AVERAGE',0.01,5,1,1,0.2,0.8,0)");
            st.executeUpdate("INSERT INTO dmat(id,data) VALUES (1,'0$')");
            st.executeUpdate("INSERT INTO tale(id,legacy_name,protein_start_seq,protein_end_seq,dna_start_seq,dna_end_seq,is_new) "
                  + "VALUES (1,'Tal1','M','M','ATG','ATG',0)");
            st.executeUpdate("INSERT INTO repeat(tale_id,repeat_ordinal,rvd,protein_seq,dna_seq) "
                  + "VALUES (1,0,'NI','" + proteinRepeat + "','" + dnaRepeat + "')");
            st.executeUpdate("INSERT INTO dmat_tale_order(ordinal,tale_id) VALUES (0,1)");
            st.executeUpdate("INSERT INTO tale_family(name,member_count,tree_newick) VALUES ('1',1,'1;')");
            st.executeUpdate("INSERT INTO tale_family_member(family_id,tale_id) VALUES ('1',1)");
        }
        try {
            TALEFamilyBuilder builder = SqliteTALEFamilyLoader.load(db.getAbsolutePath());
            assertEquals("M", builder.getFamily(0).getFamilyMembers()[0].getStart().toString());
            assertEquals(proteinRepeat, builder.getFamily(0).getFamilyMembers()[0].getRepeat(0).getRepeat().toString());
            assertEquals("ATG", builder.getFamily(0).getFamilyMembers()[0].getDnaOriginal().getStart().toString());
            assertEquals(dnaRepeat, builder.getFamily(0).getFamilyMembers()[0].getDnaOriginal().getRepeat(0).getRepeat().toString());
        } finally {
            db.delete();
        }
    }
}
