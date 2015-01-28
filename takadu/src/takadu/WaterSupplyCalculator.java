package takadu;

public class WaterSupplyCalculator {

	public static void main(String[] args) {
		if (args.length != 3)
		{
			System.out.println("wrong usage, correct usage is: WaterSupplyCalculator <supply zones file path> <supply values file path> <results file path>");
			return;
		}
		WaterSupply waterSupply = new WaterSupply();
		waterSupply.calculateWaterSupply(args[0], args[1]);
		waterSupply.exportToCsv(args[2]);
		System.out.println("finished calculating, results in: "+ args[2]);
	}

}
