package annotale.storage;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;

public class SQLiteSchema {

    private static final int SCHEMA_VERSION = 1;

    public static void ensureSchema(Connection conn) throws Exception {
        String sql;
        try (InputStream in = SQLiteSchema.class.getClassLoader().getResourceAsStream("annotale/db/schema.sql")) {
            if (in == null) {
                throw new IllegalStateException("schema resource annotale/db/schema.sql not found");
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int r;
            while ((r = in.read(buf)) != -1) {
                baos.write(buf, 0, r);
            }
            sql = new String(baos.toByteArray(), StandardCharsets.UTF_8);
        }

        try (Statement st = conn.createStatement()) {
            for (String stmt : sql.split(";")) {
                String trimmed = stmt.trim();
                if (!trimmed.isEmpty()) {
                    st.execute(trimmed);
                }
            }
        }

        ensureColumn(conn, "repeat", "masked_seq_1", "TEXT");
        ensureColumn(conn, "repeat", "masked_seq_2", "TEXT");
        ensureColumn(conn, "tale", "legacy_name", "TEXT");
        ensureColumn(conn, "tale", "assembly_id", "INTEGER");
        ensureColumn(conn, "tale", "is_pseudo", "INTEGER");
        ensureColumn(conn, "samples", "legacy_strain_name", "TEXT");
        ensureColumn(conn, "samples", "strain_name", "TEXT");
        ensureColumn(conn, "samples", "collection_date", "TEXT");
        ensureColumn(conn, "samples", "legacy_taxon_id", "INTEGER");
        ensureColumn(conn, "samples", "taxon_id", "INTEGER");
        ensureColumn(conn, "assembly", "accession_type", "TEXT");
        ensureColumn(conn, "taxonomy", "rank", "TEXT");
        ensureColumn(conn, "taxonomy", "raw_name", "TEXT");
        ensureColumn(conn, "taxonomy", "parent_id", "INTEGER");

        try (Statement st = conn.createStatement()) {
            st.execute("INSERT OR IGNORE INTO schema_migrations(version) VALUES (" + SCHEMA_VERSION + ")");
            st.execute("UPDATE samples SET legacy_strain_name=COALESCE(legacy_strain_name, strain_name) "
                  + "WHERE legacy_strain_name IS NULL AND strain_name IS NOT NULL");
            st.execute("UPDATE samples SET strain_name=NULL "
                  + "WHERE legacy_strain_name IS NOT NULL AND strain_name=legacy_strain_name");
        }
    }

    private static void ensureColumn(Connection conn, String table, String column, String type) throws Exception {
        if (columnExists(conn, table, column)) {
            return;
        }
        try (Statement st = conn.createStatement()) {
            st.execute("ALTER TABLE " + table + " ADD COLUMN " + column + " " + type);
        }
    }

    private static boolean columnExists(Connection conn, String table, String column) throws Exception {
        try (Statement st = conn.createStatement();
             java.sql.ResultSet rs = st.executeQuery("PRAGMA table_info('" + table + "')")) {
            while (rs.next()) {
                String name = rs.getString("name");
                if (column.equalsIgnoreCase(name)) {
                    return true;
                }
            }
        }
        return false;
    }
}
