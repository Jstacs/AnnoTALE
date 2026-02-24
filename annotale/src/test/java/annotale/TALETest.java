package annotale;

import de.jstacs.data.sequences.Sequence;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;

public class TALETest {

    @Test
    public void parsesAnnotationFromId() throws Exception {
        Sequence rvds = Sequence.create(RVDAlphabetContainer.SINGLETON, "HD-NI", "-");
        TALE tale = new TALE("Tal1[10-20:+1]", rvds, true, false);

        assertEquals("Tal1", tale.getId());
        assertEquals("[10-20:+1]", tale.annotationToString());
        assertTrue("annotation should set repeat count", tale.getNumberOfRepeats() > 0);
    }

    @Test
    public void preservesRvdSequenceWhenConstructedFromRvds() throws Exception {
        Sequence rvds = Sequence.create(RVDAlphabetContainer.SINGLETON, "HD-NI", "-");
        TALE tale = new TALE("Tal2", rvds, true, false);

        String observed = tale.getRvdSequence().toString("-", 0, tale.getRvdSequence().getLength());
        assertEquals("HD-NI", observed);
        assertEquals(2, tale.getNumberOfRepeats());
        assertEquals("HD", tale.getRepeat(0).getRvd());
        assertEquals("NI", tale.getRepeat(1).getRvd());
    }

    @Test
    public void taleSequenceDelegatesToUnderlyingRvdSequence() throws Exception {
        Sequence rvds = Sequence.create(RVDAlphabetContainer.SINGLETON, "NN-HD-NI", "-");
        TALE tale = new TALE("TalSeq", rvds, true, false);
        TALESequence seq = new TALESequence(tale);

        assertEquals(rvds.getAlphabetContainer(), seq.getAlphabetContainer());
        assertEquals(rvds.getLength(), seq.getLength());
        for (int i = 0; i < seq.getLength(); i++) {
            assertEquals(rvds.discreteVal(i), seq.discreteVal(i));
        }
    }

    @Test
    public void annotationToResultSetIncludesMetadata() throws Exception {
        Sequence rvds = Sequence.create(RVDAlphabetContainer.SINGLETON, "HD-NI", "-");
        TALE tale = new TALE("TalMeta", rvds, true, false);
        tale.addAnnotation("strainA", "acc1", 5, 25, true);

        assertEquals("TalMeta", tale.annotationToResultSet().getResultAt(0).getValue());
        assertEquals("strainA", tale.annotationToResultSet().getResultAt(1).getValue());
        assertEquals("acc1", tale.annotationToResultSet().getResultAt(2).getValue());
        assertEquals(5, tale.annotationToResultSet().getResultAt(3).getValue());
        assertEquals(25, tale.annotationToResultSet().getResultAt(4).getValue());
        assertEquals("+1", tale.annotationToResultSet().getResultAt(5).getValue());
    }

    @Test
    public void annotationToStringIncludesAccessionAndStrand() throws Exception {
        Sequence rvds = Sequence.create(RVDAlphabetContainer.SINGLETON, "HD-NI", "-");
        TALE tale = new TALE("TalStr", rvds, true, false);
        tale.addAnnotation("strainB", "ACC123", 100, 150, false);

        assertEquals("[ACC123: 100-150:-1]", tale.annotationToString());
    }

    @Test
    public void annotationToStringEmptyWhenUnset() throws Exception {
        Sequence rvds = Sequence.create(RVDAlphabetContainer.SINGLETON, "HD-NI", "-");
        TALE tale = new TALE("TalNoAnn", rvds, true, false);

        assertEquals("", tale.annotationToString());
    }

    @Test
    public void parseIdStripsAnnotation() throws Exception {
        Sequence rvds = Sequence.create(RVDAlphabetContainer.SINGLETON, "HD-NI", "-");
        TALE tale = new TALE("TalAnn [5-15:-1]", rvds, true, false);

        assertEquals("TalAnn", tale.getId());
        assertEquals("[5-15:-1]", tale.annotationToString());
    }

    @Test
    public void annotationToResultSetDefaultsWhenUnset() throws Exception {
        Sequence rvds = Sequence.create(RVDAlphabetContainer.SINGLETON, "HD-NI", "-");
        TALE tale = new TALE("TalDefaults", rvds, true, false);

        assertEquals("TalDefaults", tale.annotationToResultSet().getResultAt(0).getValue());
        assertEquals("", tale.annotationToResultSet().getResultAt(1).getValue());
        assertEquals("", tale.annotationToResultSet().getResultAt(2).getValue());
        assertEquals(-1, tale.annotationToResultSet().getResultAt(3).getValue());
        assertEquals(-1, tale.annotationToResultSet().getResultAt(4).getValue());
        assertEquals("", tale.annotationToResultSet().getResultAt(5).getValue());
    }

    @Test
    public void repeatAccessMatchesCount() throws Exception {
        Sequence rvds = Sequence.create(RVDAlphabetContainer.SINGLETON, "HD-NI", "-");
        TALE tale = new TALE("TalRepeats", rvds, true, false);

        assertEquals(2, tale.getNumberOfRepeats());
        assertNotNull(tale.getRepeat(0));
        assertNotNull(tale.getRepeat(1));
    }

    @Test
    public void consensusBasedConstructionKeepsRvdsAndTypes() throws Exception {
        TALE.Repeat[] reps = new TALE.Repeat[]{
              new TALE.Repeat(TALEConsensus.repeat),
              new TALE.Repeat(TALEConsensus.repeat),
              new TALE.Repeat(TALEConsensus.lastRepeat)
        };
        TALE tale = new TALE("TalConsensus", TALEConsensus.start, reps, TALEConsensus.end);

        assertEquals(3, tale.getNumberOfRepeats());
        assertEquals(TALE.Type.NORMAL, tale.getRepeat(0).getType());
        assertFalse(tale.containsAberrantRepeat());

        Sequence rvdSeq = tale.getRVDSequence(TALEConsensus.repeat, TALEConsensus.lastRepeat);
        String rvdStr = rvdSeq.toString("-", 0, rvdSeq.getLength());
        String existing = tale.getRvdSequence().toString("-", 0, tale.getRvdSequence().getLength());
        assertEquals(existing, rvdStr);
    }

    @Test
    public void detectsAberrantRepeatLength() throws Exception {
        Sequence shortRepeat = TALEConsensus.repeat.getSubSequence(0, TALEConsensus.repeat.getLength() - 1);

        TALE.Repeat[] reps = new TALE.Repeat[]{
              new TALE.Repeat(shortRepeat),
              new TALE.Repeat(TALEConsensus.repeat),
              new TALE.Repeat(TALEConsensus.lastRepeat)
        };
        TALE tale = new TALE("TalAberrant", TALEConsensus.start, reps, TALEConsensus.end);

        assertEquals(TALE.Type.SHORT, tale.getRepeat(0).getType());
        assertTrue(tale.containsAberrantRepeat());
    }

    @Test
    public void readsTalesFromFastaParts() throws Exception {
        File start = File.createTempFile("tale-start", ".fa");
        File repeat = File.createTempFile("tale-repeat", ".fa");
        File end = File.createTempFile("tale-end", ".fa");
        start.deleteOnExit();
        repeat.deleteOnExit();
        end.deleteOnExit();

        try (FileWriter fw = new FileWriter(start)) {
            fw.write(">TalX\nATG\n");
        }
        try (FileWriter fw = new FileWriter(repeat)) {
            fw.write(">TalX\nATG\n");
        }
        try (FileWriter fw = new FileWriter(end)) {
            fw.write(">TalX\nATG\n");
        }

        TALE[] tales = TALE.readTALEs(start.getAbsolutePath(), repeat.getAbsolutePath(),
                end.getAbsolutePath());

        assertEquals(1, tales.length);
        TALE t = tales[0];
        assertEquals("TalX", t.getId());
        assertEquals(1, t.getNumberOfRepeats());
        assertNotNull(t.getStart());
        assertNotNull(t.getEnd());
        assertEquals(de.jstacs.data.alphabets.DNAAlphabetContainer.SINGLETON.getClass(),
                t.getStart().getAlphabetContainer().getClass());
    }

    @Test
    public void trimsMultipleEntriesInOrder() throws Exception {
        File start = File.createTempFile("tale-start2", ".fa");
        File repeat = File.createTempFile("tale-repeat2", ".fa");
        File end = File.createTempFile("tale-end2", ".fa");
        start.deleteOnExit();
        repeat.deleteOnExit();
        end.deleteOnExit();

        try (FileWriter fw = new FileWriter(start)) {
            fw.write(">TalA\nATG\n>TalB\nATG\n");
        }
        try (FileWriter fw = new FileWriter(repeat)) {
            fw.write(">TalA\nATG\n>TalB\nATG\n");
        }
        try (FileWriter fw = new FileWriter(end)) {
            fw.write(">TalA\nATG\n>TalB\nATG\n");
        }

        TALE[] tales = TALE.readTALEs(start.getAbsolutePath(), repeat.getAbsolutePath(),
                end.getAbsolutePath());

        assertEquals(2, tales.length);
        assertEquals("TalA", tales[0].getId());
        assertEquals("TalB", tales[1].getId());
    }

}
