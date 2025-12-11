package annotale;

import annotale.storage.SQLiteSchema;
import annotale.storage.TaleDao;
import de.jstacs.io.NonParsableException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;

public class MigrateXmlToSqlite {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: MigrateXmlToSqlite <class_definitions.xml> <sqlite.db>");
            System.exit(1);
        }

        String xmlPath = args[0];
        String dbPath = args[1];

        StringBuffer xml;
        try {
            xml = new StringBuffer(new String(Files.readAllBytes(Paths.get(xmlPath))));
        } catch (Exception e) {
            System.err.println("Failed to read XML: " + e.getMessage());
            return;
        }

        TALEFamilyBuilder builder;
        try {
            builder = new TALEFamilyBuilder(xml);
        } catch (NonParsableException e) {
            System.err.println("Failed to parse TALEFamilyBuilder from XML: " + e.getMessage());
            return;
        }

        TALE[] tales = builder.getAllTALEs();
        System.out.println("Parsed " + tales.length + " TALEs from XML.");

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            conn.setAutoCommit(false);
            SQLiteSchema.ensureSchema(conn);
            TaleDao taleDao = new TaleDao(conn);
            for (TALE tale : tales) {
                taleDao.insertTale(tale);
            }
            conn.commit();
        }

        System.out.println("Migration finished into " + dbPath);
    }
}
