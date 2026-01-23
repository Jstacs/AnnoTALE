package annotale.storage;

import annotale.RVDAlphabetContainer;
import annotale.TALE;
import de.jstacs.clustering.hierachical.ClusterTree;
import de.jstacs.data.sequences.Sequence;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusterTreeNewickTest {

    @Test
    public void roundTripsNewickWithTaleIds() throws Exception {
        Sequence rvds = Sequence.create(RVDAlphabetContainer.SINGLETON, "", "-");
        TALE t1 = new TALE("Tal1", rvds, false, false);
        TALE t2 = new TALE("Tal2", rvds, false, false);
        TALE t3 = new TALE("Tal3", rvds, false, false);

        ClusterTree<TALE> leaf1 = new ClusterTree<>(t1, 0);
        ClusterTree<TALE> leaf2 = new ClusterTree<>(t2, 1);
        ClusterTree<TALE> leaf3 = new ClusterTree<>(t3, 2);
        ClusterTree<TALE> inner = new ClusterTree<>(2.0, -1, leaf1, leaf2);
        ClusterTree<TALE> root = new ClusterTree<>(4.0, -1, inner, leaf3);

        Map<Integer, TALE> taleById = new HashMap<>();
        taleById.put(1, t1);
        taleById.put(2, t2);
        taleById.put(3, t3);
        Map<Integer, Integer> indexById = new HashMap<>();
        indexById.put(1, 0);
        indexById.put(2, 1);
        indexById.put(3, 2);

        String newick = ClusterTreeNewick.toNewickWithTaleIds(root, t -> {
            if (t == t1) return 1;
            if (t == t2) return 2;
            if (t == t3) return 3;
            return null;
        });

        ClusterTree<TALE> parsed = ClusterTreeNewick.fromNewickWithTaleIds(newick, taleById, indexById);
        String roundTrip = ClusterTreeNewick.toNewickWithTaleIds(parsed, t -> {
            if (t == t1) return 1;
            if (t == t2) return 2;
            if (t == t3) return 3;
            return null;
        });

        assertEquals(newick, roundTrip);
        assertEquals(4.0, parsed.getDistance(), 1e-9);
    }

    @Test
    public void roundsDistancesForDisplay() throws Exception {
        Sequence rvds = Sequence.create(RVDAlphabetContainer.SINGLETON, "", "-");
        TALE t1 = new TALE("Tal1", rvds, false, false);
        TALE t2 = new TALE("Tal2", rvds, false, false);

        ClusterTree<TALE> leaf1 = new ClusterTree<>(t1, 0);
        ClusterTree<TALE> leaf2 = new ClusterTree<>(t2, 1);
        ClusterTree<TALE> root = new ClusterTree<>(3.4999999999999996, -1, leaf1, leaf2);

        String newick = ClusterTreeNewick.toNewickWithTaleIds(root, t -> t == t1 ? 1 : 2);
        assertTrue(newick.contains(":3.5"));
    }
}
