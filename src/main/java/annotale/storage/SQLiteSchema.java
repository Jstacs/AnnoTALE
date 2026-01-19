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
        ensureColumn(conn, "tale", "accession_id", "INTEGER");
        ensureColumn(conn, "tale", "name_short", "TEXT");
        ensureColumn(conn, "tale", "name_suffix", "TEXT");
        ensureColumn(conn, "tale", "is_pseudo", "INTEGER");
        ensureColumn(conn, "tale", "strain_id", "INTEGER");
        ensureColumn(conn, "accession", "replicon_type", "TEXT");
        ensureColumn(conn, "accession", "strain_id", "INTEGER");
        ensureColumn(conn, "strain", "species", "TEXT");
        ensureColumn(conn, "strain", "pathovar", "TEXT");
        ensureColumn(conn, "strain", "geo_tag", "TEXT");
        ensureColumn(conn, "strain", "tax_id", "INTEGER");

        try (Statement st = conn.createStatement()) {
            st.execute("INSERT OR IGNORE INTO schema_migrations(version) VALUES (" + SCHEMA_VERSION + ")");
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
