package annotale;

import de.jstacs.io.NonParsableException;
import de.jstacs.io.XMLParser;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class StoreFamilyAlignmentBlobs {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: StoreFamilyAlignmentBlobs <class_definitions.xml> <sqlite.db>");
            System.exit(1);
        }

        String xmlPath = args[0];
        String dbPath = args[1];

        TALEFamilyBuilder builder = loadBuilder(xmlPath);

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            conn.setAutoCommit(false);
            ensureBlobColumn(conn);
            int updated = storeBlobs(conn, builder);
            conn.commit();
            System.out.println("Updated " + updated + " families with alignment blobs.");
        }
    }

    private static TALEFamilyBuilder loadBuilder(String xmlPath) throws Exception {
        String raw = new String(Files.readAllBytes(Paths.get(xmlPath)));
        String normalized = raw.replace("projects.xanthogenomes.", "annotale.");
        try {
            return new TALEFamilyBuilder(new StringBuffer(normalized));
        } catch (NonParsableException e) {
            throw new IllegalStateException("Failed to parse XML: " + e.getMessage(), e);
        }
    }

    private static void ensureBlobColumn(Connection conn) throws Exception {
        if (hasColumn(conn, "tale_family", "alignments_blob")) {
            return;
        }
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("ALTER TABLE tale_family ADD COLUMN alignments_blob TEXT");
        }
    }

    private static boolean hasColumn(Connection conn, String table, String column) throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA table_info(" + table + ")")) {
            while (rs.next()) {
                if (column.equalsIgnoreCase(rs.getString("name"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static int storeBlobs(Connection conn, TALEFamilyBuilder builder) throws Exception {
        int updated = 0;
        try (PreparedStatement ps = conn.prepareStatement(
              "UPDATE tale_family SET alignments_blob=? WHERE name=?")) {
            for (TALEFamilyBuilder.TALEFamily family : builder.getFamilies()) {
                String blob = extractAlignmentsXml(family);
                ps.setString(1, blob);
                ps.setString(2, family.getFamilyId());
                updated += ps.executeUpdate();
            }
        }
        return updated;
    }

    private static String extractAlignmentsXml(TALEFamilyBuilder.TALEFamily family) {
        try {
            StringBuffer full = family.toXML();
            StringBuffer extracted = XMLParser.extractForTag(new StringBuffer(full), "alignments");
            return extracted == null ? null : extracted.toString();
        } catch (Exception e) {
            return null;
        }
    }
}
