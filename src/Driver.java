import java.util.HashMap;
import java.util.Map;

import step1.Step1RatingMatrix;
import step2.Step2Co_concurrencyMatrix;
import step3.Step3RatingMatrixSpliter;
import step4.Step4MatrixMultiplication;


public class Driver {

	public static void main(String[] args) throws Exception{
		Map<String, String> path = new HashMap<String, String>();
		path.put("Step1Input", args[0]);
		path.put("Step1Output", "Step1Output");
		
		path.put("Step2Input", path.get("Step1Output") + "/part-r-00000");
		path.put("Step2Output", "Step2Output");
		
		path.put("Step3Input", path.get("Step1Output")+ "/part-r-00000");
		path.put("Step3Output", "Step3Output");
		
		path.put("Step4Input1", path.get("Step2Output") + "/part-r-00000");
		path.put("Step4Input2", path.get("Step3Output") + "/part-r-00000");
		path.put("Step4Output", "Step4Output");
		
		Step1RatingMatrix.run(path);
		Step2Co_concurrencyMatrix.run(path);
		Step3RatingMatrixSpliter.run(path);
		Step4MatrixMultiplication.run(path);
		System.exit(0);
	}
}
