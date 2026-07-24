package annotale;

import annotale.storage.SQLiteSchema;
import annotale.storage.TaleDao;
import annotale.storage.ClusterTreeNewick;
import annotale.alignmentCosts.RVDCosts;
import de.jstacs.algorithms.alignment.cost.AffineCosts;
import de.jstacs.algorithms.alignment.cost.Costs;
import de.jstacs.io.NonParsableException;
import de.jstacs.io.XMLParser;
import de.jstacs.utils.Pair;

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
        if (args.length != 2) {
            System.err.println("Usage: MigrateXmlToSqlite <class_definitions.xml> <sqlite.db>");
            System.exit(1);
        }

        String xmlPath = args[0];
        String dbPath = args[1];

        String normalizedXml;
        try {
            String raw = new String(Files.readAllBytes(Paths.get(xmlPath)));
            normalizedXml = normalizeLegacyXml(raw);
        } catch (Exception e) {
            System.err.println("Failed to read XML: " + e.getMessage());
            return;
        }

        TALEFamilyBuilder builder;
        try {
            builder = new TALEFamilyBuilder(new StringBuffer(normalizedXml));
        } catch (NonParsableException e) {
            System.err.println("Failed to parse TALEFamilyBuilder from XML: " + e.getMessage());
            return;
        }

        TALE[] tales = builder.getAllTALEs();
        System.out.println("Parsed " + tales.length + " TALEs from XML.");

        if (Files.exists(Paths.get(dbPath))) {
            throw new IllegalArgumentException("SQLite database already exists: " + dbPath);
        }
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            conn.setAutoCommit(true);
            try (Statement st = conn.createStatement()) {
                st.execute("PRAGMA foreign_keys=ON");
                st.execute("PRAGMA journal_mode=DELETE");
                st.execute("PRAGMA synchronous=NORMAL");
            }
            conn.setAutoCommit(false);
            SQLiteSchema.ensureSchema(conn);

            TaleDao taleDao = new TaleDao(conn);
            Map<String, Integer> taleIdMap = new HashMap<>();
            for (TALE tale : tales) {
                int dbId = taleDao.insertTale(tale);
                taleIdMap.put(tale.getId(), dbId);
            }
            insertFamilies(conn, builder, taleIdMap);
            insertBuilderState(conn, builder, taleIdMap);
            upsertDataVersion(conn);
            conn.commit();
        }

        System.out.println("Migration finished into " + dbPath);
    }

    private static void insertFamilies(Connection conn, TALEFamilyBuilder builder, Map<String, Integer> taleIdMap)
          throws Exception {
        try (PreparedStatement familyStmt = conn.prepareStatement(
                     "INSERT OR REPLACE INTO tale_family(name, member_count, tree_newick, alignment_fasta) VALUES (?,?,?,?)");
             PreparedStatement memberStmt = conn.prepareStatement(
                     "INSERT OR REPLACE INTO tale_family_member(family_id, tale_id) VALUES (?,?)")) {

            TALEFamilyBuilder.TALEFamily[] families = builder.getFamilies();
            for (TALEFamilyBuilder.TALEFamily family : families) {
                String newick = family.getTree() == null ? null
                      : ClusterTreeNewick.toNewickWithTaleIds(
                            family.getTree(), t -> taleIdMap.get(t.getId()));
                familyStmt.setString(1, family.getFamilyId());
                familyStmt.setInt(2, family.getFamilySize());
                familyStmt.setString(3, newick);
                familyStmt.setString(4, toFasta(family.getInducedMultipleAlignment(), taleIdMap));
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

    private static String toFasta(Pair<TALE[], String[]> alignment, Map<String, Integer> taleIdMap) {
        StringBuilder fasta = new StringBuilder();
        TALE[] tales = alignment.getFirstElement();
        String[] rows = alignment.getSecondElement();
        for (int i = 0; i < tales.length; i++) {
            fasta.append('>').append(taleIdMap.get(tales[i].getId())).append('\n');
            fasta.append(rows[i].replace(" ", "")).append('\n');
        }
        return fasta.toString();
    }

    private static void insertBuilderState(Connection conn, TALEFamilyBuilder builder,
          Map<String, Integer> taleIdMap) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                     "INSERT OR REPLACE INTO analysis_config(id, alignment_type, cut, extra_gap_open, extra_gap_ext, linkage, pval, "
                           + "cost_affine_open, cost_affine_extend, cost_rvd_gap, cost_rvd_twelve, cost_rvd_thirteen, cost_rvd_bonus) "
                           + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
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
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                     "INSERT OR REPLACE INTO dmat(id, data) VALUES (1,?)")) {
            ps.setString(1, extractDmat(builder.toXML()));
            ps.executeUpdate();
        }
        insertTaleOrder(conn, taleOrderIds(builder, taleIdMap));
    }

    private static void insertTaleOrder(Connection conn, int[] order)
          throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
              "INSERT OR REPLACE INTO dmat_tale_order(ordinal, tale_id) VALUES (?,?)")) {
            for (int i = 0; i < order.length; i++) {
                ps.setInt(1, i);
                ps.setInt(2, order[i]);
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
                return params;
            }
        }
        throw new IllegalArgumentException("SQLite migration supports only AffineCosts wrapping RVDCosts");
    }

    private static int[] taleOrderIds(TALEFamilyBuilder builder, Map<String, Integer> taleIdMap)
          throws SQLException {
        TALE[] all = builder.getAllTALEs();
        int[] ids = new int[all.length];
        for (int i = 0; i < all.length; i++) {
            Integer dbId = taleIdMap.get(all[i].getId());
            if (dbId == null) {
                throw new SQLException("No tale id mapped for legacy id " + all[i].getId());
            }
            ids[i] = dbId;
        }
        return ids;
    }

    private static void upsertDataVersion(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
              "INSERT OR REPLACE INTO data_version(id, version) VALUES (1,?)")) {
            ps.setLong(1, System.currentTimeMillis());
            ps.executeUpdate();
        }
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
