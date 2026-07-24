package annotale.storage;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;

public class SQLiteSchema {

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

    }
}
