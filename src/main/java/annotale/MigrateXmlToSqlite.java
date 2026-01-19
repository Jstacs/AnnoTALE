package annotale;

import annotale.storage.SQLiteSchema;
import annotale.storage.TaleDao;
import annotale.alignmentCosts.RVDCosts;
import de.jstacs.algorithms.alignment.cost.AffineCosts;
import de.jstacs.algorithms.alignment.cost.Costs;
import de.jstacs.io.NonParsableException;
import de.jstacs.io.XMLParser;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

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
            String raw = new String(Files.readAllBytes(Paths.get(xmlPath)));
            xml = new StringBuffer(normalizeLegacyXml(raw));
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

        // make sure database is clean
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            conn.setAutoCommit(true);
            try (Statement st = conn.createStatement()) {
                st.execute("PRAGMA journal_mode=TRUNCATE");
                st.execute("PRAGMA synchronous=NORMAL");
            }
            conn.setAutoCommit(false);
            SQLiteSchema.ensureSchema(conn);
            try (Statement st = conn.createStatement()) {
                st.executeUpdate("DELETE FROM family_member");
                st.executeUpdate("DELETE FROM family");
                st.executeUpdate("DELETE FROM analysis_config");
                st.executeUpdate("DELETE FROM dmat_tale_order");
                st.executeUpdate("DELETE FROM repeat");
                st.executeUpdate("DELETE FROM tale");
                st.executeUpdate("DELETE FROM accession");
                st.executeUpdate("DELETE FROM strain");
            }
            TaleDao taleDao = new TaleDao(conn);
            Map<String, Integer> taleIdMap = new HashMap<>();
            for (TALE tale : tales) {
                int dbId = taleDao.insertTale(tale);
                taleIdMap.put(tale.getId(), dbId);
            }
            insertFamilies(conn, builder, taleIdMap);
            insertBuilderState(conn, builder);
            conn.commit();
        }

        System.out.println("Migration finished into " + dbPath);
    }

    private static void insertFamilies(Connection conn, TALEFamilyBuilder builder, Map<String, Integer> taleIdMap)
          throws Exception {
        try (PreparedStatement familyStmt = conn.prepareStatement(
                     "INSERT OR REPLACE INTO family(name, member_count, tree_newick) VALUES (?,?,?)");
             PreparedStatement memberStmt = conn.prepareStatement(
                     "INSERT OR REPLACE INTO family_member(family_id, tale_id) VALUES (?,?)")) {

            TALEFamilyBuilder.TALEFamily[] families = builder.getFamilies();
            for (TALEFamilyBuilder.TALEFamily family : families) {
                String newick = family.getTree() == null ? null : family.getTree().toNewick();
                familyStmt.setString(1, family.getFamilyId());
                familyStmt.setInt(2, family.getFamilySize());
                familyStmt.setString(3, newick);
                familyStmt.executeUpdate();

                TALE[] members = family.getFamilyMembers();
                for (int i = 0; i < members.length; i++) {
                    Integer taleDbId = taleIdMap.get(members[i].getId());
                    if (taleDbId == null) {
                        throw new SQLException("No tale id mapped for legacy id " + members[i].getId());
                    }
                    memberStmt.setString(1, family.getFamilyId());
                    memberStmt.setInt(2, taleDbId);
                    memberStmt.addBatch();
                }
            }

            memberStmt.executeBatch();
        }
    }

    private static void insertBuilderState(Connection conn, TALEFamilyBuilder builder) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                     "INSERT OR REPLACE INTO analysis_config(id, alignment_type, cut, extra_gap_open, extra_gap_ext, linkage, pval, "
                           + "cost_affine_open, cost_affine_extend, cost_rvd_gap, cost_rvd_twelve, cost_rvd_thirteen, cost_rvd_bonus, "
                           + "reserved_names) "
                           + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            ps.setString(1, "default");
            ps.setString(2, builder.getAlignmentType() == null ? null : builder.getAlignmentType().name());
            ps.setObject(3, builder.getCut());
            ps.setObject(4, builder.getExtraGapOpening());
            ps.setObject(5, builder.getExtraGapExtension());
            ps.setString(6, builder.getLinkage() == null ? null : builder.getLinkage().name());
            ps.setObject(7, builder.getPVal());
            CostParams costParams = extractCostParams(builder.getCosts());
            ps.setObject(8, costParams.affineOpen);
            ps.setObject(9, costParams.affineExtend);
            ps.setObject(10, costParams.rvdGap);
            ps.setObject(11, costParams.rvdTwelve);
            ps.setObject(12, costParams.rvdThirteen);
            ps.setObject(13, costParams.rvdBonus);
            ps.setString(14, builder.getReservedNames() == null ? null : String.join(",", builder.getReservedNames()));
            ps.executeUpdate();
        }

        try (PreparedStatement ps = conn.prepareStatement(
                     "INSERT OR REPLACE INTO dmat(config_id, data) VALUES (?,?)")) {
            ps.setString(1, "default");
            ps.setString(2, extractDmat(builder.toXML()));
            ps.executeUpdate();
        }
        insertTaleOrder(conn, "default", taleOrder(builder));
    }

    private static void insertTaleOrder(Connection conn, String configId, String[] order)
          throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
              "INSERT OR REPLACE INTO dmat_tale_order(config_id, ordinal, tale_name) VALUES (?,?,?)")) {
            for (int i = 0; i < order.length; i++) {
                ps.setString(1, configId);
                ps.setInt(2, i);
                ps.setString(3, order[i]);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static String extractDmat(StringBuffer builderXml) {
        try {
            StringBuffer extracted = XMLParser.extractForTag(new StringBuffer(builderXml), "dmatStore");
            if (extracted == null) {
                return null;
            }
            String value = extracted.toString();
            String open = "<dmatStore>";
            String close = "</dmatStore>";
            int start = value.indexOf(open);
            int end = value.lastIndexOf(close);
            if (start >= 0 && end > start) {
                return value.substring(start + open.length(), end);
            }
            return value;
        } catch (Exception ignored) {
            return null;
        }
    }

    private static CostParams extractCostParams(Costs costs) {
        CostParams params = new CostParams();
        if (costs instanceof AffineCosts) {
            AffineCosts affine = (AffineCosts) costs;
            params.affineOpen = affine.getInsertCosts();
            params.affineExtend = affine.getElongateInsertCosts();
            Costs inner = affine.getInternalCosts();
            if (inner instanceof RVDCosts) {
                RVDCosts rvd = (RVDCosts) inner;
                params.rvdGap = rvd.getGapCosts();
                params.rvdTwelve = rvd.getTwelve();
                params.rvdThirteen = rvd.getThirteen();
                params.rvdBonus = rvd.getBonus();
            }
        }
        return params;
    }

    private static String[] taleOrder(TALEFamilyBuilder builder) {
        TALE[] all = builder.getAllTALEs();
        String[] ids = new String[all.length];
        for (int i = 0; i < all.length; i++) {
            ids[i] = all[i].getId();
        }
        return ids;
    }

    /**
     * Map legacy class names from the original XML into the in-repo equivalents.
     */
    private static String normalizeLegacyXml(String xml) {
        return xml.replace("projects.xanthogenomes.", "annotale.");
    }

    private static final class CostParams {
        Double affineOpen;
        Double affineExtend;
        Double rvdGap;
        Double rvdTwelve;
        Double rvdThirteen;
        Double rvdBonus;
    }
}
