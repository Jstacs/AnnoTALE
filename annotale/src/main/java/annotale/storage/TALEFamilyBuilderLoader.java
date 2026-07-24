package annotale.storage;

import annotale.TALEFamilyBuilder;
import de.jstacs.parameters.FileParameter.FileRepresentation;

public class TALEFamilyBuilderLoader {

    private TALEFamilyBuilderLoader() {}

    public static TALEFamilyBuilder load(FileRepresentation file) throws Exception {
        if (file == null) {
            throw new IllegalArgumentException("FileRepresentation is null");
        }
        String filename = file.getFilename();
        if (filename != null && filename.toLowerCase().endsWith(".db")) {
            return SqliteTALEFamilyLoader.load(filename);
        }
        return new TALEFamilyBuilder(new StringBuffer(file.getContent()));
    }
}
