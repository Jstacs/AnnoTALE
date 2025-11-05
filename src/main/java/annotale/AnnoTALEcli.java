package annotale;

import de.jstacs.tools.JstacsTool;
import de.jstacs.tools.ui.cli.CLI;
import projects.tals.prediction.QuickTBSPredictionTool;
import projects.tals.rnaseq.DerTALE;
import annotale.tools.ClassAssignmentTool;
import annotale.tools.ClassBuilderTool;
import annotale.tools.ClassPresenceTool;
import annotale.tools.LoadAndViewClassesTool;
import annotale.tools.PredictAndIntersectTargetsTool;
import annotale.tools.RenameTool;
import annotale.tools.TALEAnalysisTool;
import annotale.tools.TALEComparisonTool;
import annotale.tools.TALEPredictionTool;


public class AnnoTALEcli {

    public static void main(String[] args) throws Exception {

        JstacsTool[] tools = new JstacsTool[]{
              new TALEPredictionTool(),
              new TALEAnalysisTool(),
              new ClassBuilderTool(),
              new LoadAndViewClassesTool(),
              new ClassAssignmentTool(),
              new RenameTool(),
              new PredictAndIntersectTargetsTool(),
              new ClassPresenceTool(),
              new TALEComparisonTool(),
              new QuickTBSPredictionTool(),
              new DerTALE()};

        CLI cli = new CLI(tools);

        cli.run(args);

    }

}
