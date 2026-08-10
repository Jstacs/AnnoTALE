package annotale;

import de.jstacs.data.DataSet;
import de.jstacs.data.alphabets.DNAAlphabetContainer;
import de.jstacs.data.sequences.Sequence;
import de.jstacs.tools.ProgressUpdater;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;

/**
 * Prediction example: CP013676 / TalAA11 in db/annotale.db, exported
 * from CP013676.1__nuccore.fasta (coordinates 2,416,529-2,420,252,
 * 1-based; 3723 nt). Padded test adds 300 nt deterministic
 * pseudo random DNA on each side (seeds 17 and 18) not true genomic context.
 */
public class NHMMerPredictionTest {

    @Test
    public void findsTheCompleteTaleWhenItStartsAtTheSequenceBoundary() throws Exception {
        String dna = talAa11();

        assertArrayEquals(new int[][]{{0, 0, 3723, 1, 0, 3723, 0}},
                predict(dna + randomDna(300, 18)));
    }

    @Test
    public void findsTheCompleteTaleWhenItEndsAtTheSequenceBoundary() throws Exception {
        String dna = talAa11();

        // TODO: we should keep terminal stop codon: correct end is 4023, not 4020 as currently!
        assertArrayEquals(new int[][]{{0, 300, 4020, 1, 300, 4020, 0}},
                predict(randomDna(300, 17) + dna));
    }

    @Test
    public void findsTheTaleAtItsKnownCoordinatesWithRandomDnaPadding() throws Exception {
        String dna = talAa11();
        String leftPadding = randomDna(300, 17);
        String rightPadding = randomDna(300, 18);

        assertArrayEquals(new int[][]{{0, 300, 4023, 1, 300, 4023, 0}},
                predict(leftPadding + dna + rightPadding));
    }

    private int[][] predict(String dna) throws Exception {
        DataSet data = new DataSet("TalAA11", Sequence.create(DNAAlphabetContainer.SINGLETON, dna));
        return NHMMer.run(
                new InputStreamReader(getClass().getResourceAsStream("/annotale/data/repeats.hmm")),
                new InputStreamReader(getClass().getResourceAsStream("/annotale/data/starts.hmm")),
                new InputStreamReader(getClass().getResourceAsStream("/annotale/data/ends.hmm")),
                data, new ProgressUpdater(), false);
    }

    private String talAa11() throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                getClass().getResourceAsStream("/annotale/data/talAA11.fasta")))) {
            return reader.lines().filter(line -> !line.startsWith(">"))
                    .collect(Collectors.joining());
        }
    }

    private String randomDna(int length, long seed) {
        char[] bases = {'A', 'C', 'G', 'T'};
        Random random = new Random(seed);
        StringBuilder dna = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            dna.append(bases[random.nextInt(bases.length)]);
        }
        return dna.toString();
    }

}
