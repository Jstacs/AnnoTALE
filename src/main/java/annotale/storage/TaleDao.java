package annotale.storage;

import annotale.TALE;
import annotale.TALE.Repeat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TaleDao {

    private final Connection conn;

    public TaleDao(Connection conn) {
        this.conn = conn;
    }

    public void insertTale(TALE tale) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
              "INSERT OR REPLACE INTO tale(id,strain,accession,start_pos,end_pos,strand,is_new) VALUES (?,?,?,?,?,?,?)")) {
            ps.setString(1, tale.getId());
            ps.setString(2, tale.getStrain());
            ps.setString(3, tale.getAccession());
            ps.setObject(4, tale.getStartPos());
            ps.setObject(5, tale.getEndPos());
            ps.setObject(6, tale.getStrand() == null ? null : (tale.getStrand() ? 1 : -1));
            ps.setObject(7, tale.isNew());
            ps.executeUpdate();
        }

        try (PreparedStatement ps = conn.prepareStatement(
              "INSERT INTO repeat(tale_id, ordinal, rvd, rvd_pos, rvd_len, repeat_seq, masked_seq1, masked_seq2) VALUES (?,?,?,?,?,?,?,?)")) {
            Repeat[] reps = tale.getRepeats();
            for (int i = 0; i < reps.length; i++) {
                Repeat r = reps[i];
                ps.setString(1, tale.getId());
                ps.setInt(2, i);
                ps.setString(3, r.getRvd());
                ps.setObject(4, r.getRvdPosition());
                ps.setObject(5, r.getRvdLength());
                ps.setString(6, r.getRepeat() == null ? null : r.getRepeat().toString());
                ps.setString(7, r.getMaskedRepeats() == null || r.getMaskedRepeats().length == 0 ? null
                      : r.getMaskedRepeats()[0].toString());
                ps.setString(8,
                      r.getMaskedRepeats() == null || r.getMaskedRepeats().length < 2 ? null
                            : r.getMaskedRepeats()[1].toString());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }
}
