package annotale.storage;

import java.sql.Connection;
import java.sql.Statement;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

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
            st.executeUpdate(sql);
        }
    }
}
