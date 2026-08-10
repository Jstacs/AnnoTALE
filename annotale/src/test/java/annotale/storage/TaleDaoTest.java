package annotale.storage;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TaleDaoTest {

    @Test
    public void removesTerminalUnnamedPlasmidDescriptorsFromSampleNames() {
        assertEquals("X campestris pv. incanae 5057", TaleDao.normalizeSampleName(
              "X campestris pv. incanae 5057 plasmid unnamed"));
        assertEquals("X campestris pv. campestris 12112", TaleDao.normalizeSampleName(
              "X campestris pv. campestris 12112 plasmid unnamed2"));
        assertEquals(null, TaleDao.normalizeSampleName(null));
    }

    @Test
    public void preservesCompleteStrainIdentifiers() {
        assertEquals("WHRI 5233", TaleDao.parseStrain("Xoo WHRI 5233"));
        assertEquals("Aw12879", TaleDao.parseStrain("Xanthomonas citri subsp. citri Aw12879"));
        assertEquals("12-2", TaleDao.parseStrain("Xanthomonas citri pv. glycines str. 12-2"));
    }
}

