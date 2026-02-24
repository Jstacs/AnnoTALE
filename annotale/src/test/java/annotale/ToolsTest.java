package annotale;

import de.jstacs.algorithms.alignment.Alignment.AlignmentType;
import de.jstacs.algorithms.alignment.PairwiseStringAlignment;
import de.jstacs.data.AlphabetContainer;
import de.jstacs.data.DataSet;
import de.jstacs.data.alphabets.DNAAlphabetContainer;
import de.jstacs.data.sequences.Sequence;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ToolsTest {

    @Test
    public void translatesFrameZeroWithDefaultCode() throws Exception {
        Sequence dna = Sequence.create(DNAAlphabetContainer.SINGLETON, "ATGGTGCTGATCTAG");
        Sequence protein = Tools.Translator.DEFAULT.translate(dna, 0);

        assertEquals("MVLI*", protein.toString());
    }

    @Test
    public void translatesWithReadingFrameOffset() throws Exception {
        Sequence dna = Sequence.create(DNAAlphabetContainer.SINGLETON, "AATATGTTT");
        Sequence protein = Tools.Translator.DEFAULT.translate(dna, 1);

        assertEquals("IC", protein.toString());
    }

    @Test
    public void ignoresTrailingBasesThatDoNotFormCodon() throws Exception {
        Sequence dna = Sequence.create(DNAAlphabetContainer.SINGLETON, "ATGGT");
        Sequence protein = Tools.Translator.DEFAULT.translate(dna, 0);

        assertEquals("M", protein.toString());
    }

    @Test
    public void translatesInternalStopCodon() throws Exception {
        Sequence dna = Sequence.create(DNAAlphabetContainer.SINGLETON, "ATGTAGGAA");
        Sequence protein = Tools.Translator.DEFAULT.translate(dna, 0);

        assertEquals("M*E", protein.toString());
    }

    @Test
    public void returnsEmptySequenceWhenFrameExceedsLength() throws Exception {
        Sequence dna = Sequence.create(DNAAlphabetContainer.SINGLETON, "ATG");
        Sequence protein = Tools.Translator.DEFAULT.translate(dna, 2);

        assertEquals(0, protein.getLength());
    }

    @Test
    public void translatesFrameOneWithTrailingBaseDropped() throws Exception {
        Sequence dna = Sequence.create(DNAAlphabetContainer.SINGLETON, "GATGAAA");
        Sequence protein = Tools.Translator.DEFAULT.translate(dna, 1);

        assertEquals("MK", protein.toString());
    }

    @Test
    public void returnsEmptyOnTooShortSequence() throws Exception {
        Sequence dna = Sequence.create(DNAAlphabetContainer.SINGLETON, "AT");
        Sequence protein = Tools.Translator.DEFAULT.translate(dna, 0);

        assertEquals(0, protein.getLength());
    }

    @Test
    public void translateArrayPreservesOrder() throws Exception {
        Sequence[] tales = new Sequence[]{
                Sequence.create(DNAAlphabetContainer.SINGLETON, "ATGGTG"),
                Sequence.create(DNAAlphabetContainer.SINGLETON, "ATGAAA")
        };

        Sequence[] proteins = Tools.translate(tales, Tools.Translator.DEFAULT);

        assertEquals("MV", proteins[0].toString());
        assertEquals("MK", proteins[1].toString());
    }

    @Test
    public void translateArrayHandlesEmptyInput() throws Exception {
        Sequence[] tales = new Sequence[0];
        Sequence[] proteins = Tools.translate(tales, Tools.Translator.DEFAULT);

        assertEquals(0, proteins.length);
    }

    @Test
    public void extractRVDsPullsExpectedSubstrings() throws Exception {
        Sequence s1 = Sequence.create(DNAAlphabetContainer.SINGLETON, "ACGTACGTACGTAC");
        Sequence s2 = Sequence.create(DNAAlphabetContainer.SINGLETON, "TTTTTTTTTTTTTT");

        DataSet ds = new DataSet("rvds", new Sequence[]{s1, s2});

        DataSet extracted = Tools.extractRVDs(ds);

        assertEquals(2, extracted.getNumberOfElements());
        assertEquals("TA", extracted.getElementAt(0).toString());
        assertEquals("TT", extracted.getElementAt(1).toString());
    }

    @Test(expected = de.jstacs.data.EmptyDataSetException.class)
    public void extractRVDsThrowsOnEmptyDataset() throws Exception {
        DataSet ds = new DataSet("rvds", new Sequence[0]);
        Tools.extractRVDs(ds);
    }

    @Test
    public void rvdAlphabetHasFullPairSpaceAndCanResolveSymbols() throws Exception {
        AlphabetContainer prot = Tools.Translator.DEFAULT.getProteinAlphabet();
        AlphabetContainer rvd = RVDAlphabetContainer.SINGLETON;

        double expectedPairs = prot.getAlphabetLengthAt(0) * prot.getAlphabetLengthAt(0);
        assertEquals(expectedPairs, rvd.getAlphabetLengthAt(0), 0.0);

        int code = (int) rvd.getCode(0, "HD");
        assertEquals("HD", rvd.getSymbol(0, code));
    }

    @Test
    public void buildsConsensusWithoutGaps() throws Exception {
        AlphabetContainer alphabet = Tools.Translator.DEFAULT.getProteinAlphabet();

        Sequence s1 = Sequence.create(alphabet, "A-C");
        Sequence s2 = Sequence.create(alphabet, "ACC");

        DataSet alignment = new DataSet("aln", new Sequence[]{s1, s2});

        Sequence consensus = Tools.getConsensusSequence(alignment);

        assertEquals("ACC", consensus.toString());
    }

    @Test
    public void dropsGapOnlyPositions() throws Exception {
        AlphabetContainer alphabet = Tools.Translator.DEFAULT.getProteinAlphabet();

        Sequence s1 = Sequence.create(alphabet, "-A-");
        Sequence s2 = Sequence.create(alphabet, "-A-");
        Sequence s3 = Sequence.create(alphabet, "---");

        DataSet alignment = new DataSet("aln", new Sequence[]{s1, s2, s3});

        Sequence consensus = Tools.getConsensusSequence(alignment);

        assertEquals("A", consensus.toString());
    }

    @Test
    public void keepsMostFrequentSymbols() throws Exception {
        AlphabetContainer alphabet = Tools.Translator.DEFAULT.getProteinAlphabet();

        Sequence s1 = Sequence.create(alphabet, "ABC");
        Sequence s2 = Sequence.create(alphabet, "ADC");
        Sequence s3 = Sequence.create(alphabet, "ADC");

        DataSet alignment = new DataSet("aln", new Sequence[]{s1, s2, s3});

        Sequence consensus = Tools.getConsensusSequence(alignment);

        assertEquals("ADC", consensus.toString());
    }

    @Test
    public void breaksTiesByTakingLastMaxIndex() throws Exception {
        AlphabetContainer alphabet = Tools.Translator.DEFAULT.getProteinAlphabet();

        Sequence s1 = Sequence.create(alphabet, "A");
        Sequence s2 = Sequence.create(alphabet, "C");

        DataSet alignment = new DataSet("aln", new Sequence[]{s1, s2});

        Sequence consensus = Tools.getConsensusSequence(alignment);

        assertEquals("C", consensus.toString());
    }

    @Test
    public void singleSequenceConsensusIsIdentity() throws Exception {
        AlphabetContainer alphabet = Tools.Translator.DEFAULT.getProteinAlphabet();

        Sequence s1 = Sequence.create(alphabet, "MVI");

        DataSet alignment = new DataSet("aln", new Sequence[]{s1});

        Sequence consensus = Tools.getConsensusSequence(alignment);

        assertEquals("MVI", consensus.toString());
    }

    @Test
    public void allGapsReturnEmptyConsensus() throws Exception {
        AlphabetContainer alphabet = Tools.Translator.DEFAULT.getProteinAlphabet();

        Sequence s1 = Sequence.create(alphabet, "---");
        Sequence s2 = Sequence.create(alphabet, "---");

        DataSet alignment = new DataSet("aln", new Sequence[]{s1, s2});

        Sequence consensus = Tools.getConsensusSequence(alignment);

        assertEquals(0, consensus.getLength());
    }

    @Test
    public void alignsIdenticalSequencesWithoutGaps() throws Exception {
        AlphabetContainer prot = Tools.Translator.DEFAULT.getProteinAlphabet();
        Sequence s1 = Sequence.create(prot, "ACD");
        Sequence s2 = Sequence.create(prot, "ACD");

        PairwiseStringAlignment al = Tools.Aligner.DEFAULT.align(s1, s2, AlignmentType.GLOBAL);

        assertNotNull(al);
        String a0 = al.getAlignedString(0).replace("-", "").replace(" ", "");
        String a1 = al.getAlignedString(1).replace("-", "").replace(" ", "");
        assertEquals("ACD", a0);
        assertEquals("ACD", a1);
    }

    @Test
    public void alignmentCostIsSymmetric() throws Exception {
        AlphabetContainer prot = Tools.Translator.DEFAULT.getProteinAlphabet();
        Sequence s1 = Sequence.create(prot, "ACD");
        Sequence s2 = Sequence.create(prot, "ADC");

        PairwiseStringAlignment al12 = Tools.Aligner.DEFAULT.align(s1, s2, AlignmentType.GLOBAL);
        PairwiseStringAlignment al21 = Tools.Aligner.DEFAULT.align(s2, s1, AlignmentType.GLOBAL);

        assertEquals(al12.getCost(), al21.getCost(), 1e-9);
    }

    @Test
    public void identicalSequencesHaveLowerCostThanMismatch() throws Exception {
        AlphabetContainer prot = Tools.Translator.DEFAULT.getProteinAlphabet();
        Sequence s1 = Sequence.create(prot, "ACD");
        Sequence s2 = Sequence.create(prot, "ACD");
        Sequence s3 = Sequence.create(prot, "ACX");

        PairwiseStringAlignment alSame = Tools.Aligner.DEFAULT.align(s1, s2, AlignmentType.GLOBAL);
        PairwiseStringAlignment alMismatch = Tools.Aligner.DEFAULT.align(s1, s3, AlignmentType.GLOBAL);

        assertTrue(alSame.getCost() <= alMismatch.getCost());
    }

    @Test
    public void introducesGapForInsertions() throws Exception {
        AlphabetContainer prot = Tools.Translator.DEFAULT.getProteinAlphabet();
        Sequence s1 = Sequence.create(prot, "ACD");
        Sequence s2 = Sequence.create(prot, "ACXD");

        PairwiseStringAlignment al = Tools.Aligner.DEFAULT.align(s1, s2, AlignmentType.GLOBAL);

        String aligned0 = al.getAlignedString(0);
        String aligned1 = al.getAlignedString(1);
        assertTrue(aligned0.contains("-") || aligned1.contains("-"));
    }

}
