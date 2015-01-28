package takadu;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

/**
 * this class represents water supply information, it includes the zones hierarchy and also water supply data, this class let you calculate
 * aggregated data and also export water supply data to a csv file.
 */
public class WaterSupply {

	private static final String NULL = "null";
	protected static final int NULL_VALUE = -1;
	protected static final int THREAD_TIMEOUT = 60000;
	// this is a variable that is used to store all the water supply data, for each date we encounter in the supply values file we add data for all the zones in that
	// date and we update the corresponding zone in that date to it's actual data.
	// this member map date in string to an hash map the maps zone name to Supply value.
	protected HashMap<String, HashMap<String, SupplyValue>> datesValuesMap;
	protected ZoneNode zonesTreeRoot;
	protected ArrayList<SupplyZone> supplyZoneLines; //we can delete SupplyZone object and just use String that contains zone name and parent name, and split it later.
	protected HashMap<String , ZoneNode> zoneNodeMap;


	public WaterSupply()
	{
		datesValuesMap = new HashMap<String, HashMap<String, SupplyValue>>();
		supplyZoneLines = new ArrayList<SupplyZone>();
		zoneNodeMap = new HashMap<String , ZoneNode>();
	}

	/**
	 * this method calculates aggregated data of water supply.
	 */
	public void calculateWaterSupply(String supplyZonesFilePath, String supplyValuesFilePath) {
		parseZones(new File(supplyZonesFilePath)); //fills the supplyZoneLines list.

		fillZoneNodeMap();

		buildHirarchyTree();

		parseValues(new File(supplyValuesFilePath));

		fillMissingValues();


	}

	/**
	 * this method extracts the water supply data to a requested file
	 * @param outputFilePath the output file path to write the data into
	 */
	public void exportToCsv(String outputFilePath) {
		BufferedWriter outputWriter = null;
		try {
			File outputFile = new File(outputFilePath);
			if (!outputFile.exists()) outputFile.createNewFile();
			outputWriter = new BufferedWriter(new FileWriter(outputFile.getAbsoluteFile()));
			String valString,typeString;
			SupplyValue supplyValue;
			for (String date : datesValuesMap.keySet())
			{
				for (String zone : datesValuesMap.get(date).keySet())
				{
					supplyValue = datesValuesMap.get(date).get(zone);
					valString = (supplyValue.getSupplyValue() == NULL_VALUE) ? NULL : String.valueOf(supplyValue.getSupplyValue());
					if (valString.equals(NULL))
					{
						typeString = NULL;
					}
					else
					{
						typeString = supplyValue.isActual() ? "actual" : "aggregated";
					}
					outputWriter.write(zone+","+date+","+valString+","+typeString+"\n");
				}
			}
		} catch (IOException e) {
			System.err.println("IOException: got an error while trying to work with output file");
			e.printStackTrace();
		}
		finally
		{
			try {
				if (outputWriter != null) outputWriter.close();
			} catch (IOException e) {}
		}
	}

	/**
	 * this method reads supply zones file and stores it in a corresponding data structure
	 * @param file supply zones file
	 */
	protected void parseZones(File file) {
		Scanner scanner = null;
		try {
			scanner = new Scanner(file);
			String line;
			if(scanner.hasNext()) 
				line = scanner.nextLine();
			while (scanner.hasNextLine()) {
				line = scanner.nextLine();
				addToSupplyZoneLines(line);
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		finally
		{
			if (scanner != null) scanner.close();
		}
	}

	/**
	 * this method read supply values file and creates a suitable data structure to fill the data collected as described in datesValuesMap member documentation above.
	 * @param file the supply values file
	 */
	protected void parseValues(File file){
		try {
			Scanner scanner = new Scanner(file);
			String line;
			if(scanner.hasNext()) 
				line = scanner.nextLine();
			while (scanner.hasNextLine()) {
				line = scanner.nextLine();
				addToSupplyValues(line);
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * this method adds a single supply value line
	 * @param line the line to add
	 */
	protected void addToSupplyValues(String line) {
		String[] splitedLine = line.split(",");
		String supplyName = splitedLine[0];
		int supplyValue = Integer.parseInt(splitedLine[2]);
		String supplydate = splitedLine[1];
		if(!datesValuesMap.containsKey(supplydate)){
			HashMap<String, SupplyValue> values = new HashMap<String, SupplyValue>();
			for(SupplyZone s : supplyZoneLines){
				values.put(s.getZoneName(), new SupplyValue(NULL_VALUE, false));
			}
			datesValuesMap.put(supplydate, values);
		}
		datesValuesMap.get(supplydate).get(supplyName).setSupplyValue(supplyValue);
		datesValuesMap.get(supplydate).get(supplyName).setActual(true);
	}

	/**
	 * this method adds a line to supplyZoneLine member as described in datesValuesMap member documentation above
	 * @param line the line in the file to add
	 */
	protected void addToSupplyZoneLines(String line) {
		String[] splitedLine = line.split(",");
		String zoneName = splitedLine[0];
		String parentZoneName = splitedLine.length == 2 ? splitedLine[1] : null;	 
		supplyZoneLines.add(new SupplyZone(zoneName, parentZoneName));
	}

	/**
	 * this method fills all the possible aggregated values.
	 */
	protected void fillMissingValues() {
		ArrayList<Thread> fillDateThreads = new ArrayList<Thread>();
		fillDateValuesJob fillDateJob;
		Thread currentThreadToAdd;
		for (String date : datesValuesMap.keySet())
		{
			HashMap<String, SupplyValue> dateValue = datesValuesMap.get(date);
			fillDateJob = new fillDateValuesJob(dateValue, zonesTreeRoot);
			currentThreadToAdd = new Thread(fillDateJob);
			currentThreadToAdd.start();
			fillDateThreads.add(currentThreadToAdd);
		}
		for (Thread fillThread : fillDateThreads)
		{
			try {
				fillThread.join(THREAD_TIMEOUT);
			} catch (InterruptedException e) {}
			if (fillThread.isAlive())
			{
				System.err.println("one of the fill missing values threads didn't finish in timeout, results may be wrong");
			}
		}
	}

	/**
	 * this class represents a job that calculates all aggregated data for a certain date
	 */
	static class fillDateValuesJob implements Runnable
	{
		HashMap<String, SupplyValue> dateVal;
		ZoneNode currNode;
		
		public fillDateValuesJob(HashMap<String, SupplyValue> dateVal,
				ZoneNode currNode) {
			super();
			this.dateVal = dateVal;
			this.currNode = currNode;
		}

		public void run() {
			fillDateValues(dateVal, currNode);
		}
		
		// this is a recursive function to fill all the water supply values in a certain date data according to an zone hierarchy tree
		protected int fillDateValues(HashMap<String, SupplyValue> dateValue,ZoneNode currentNode) {
			if (currentNode == null) return NULL_VALUE;
			SupplyValue supplyValue = dateValue.get(currentNode.getZoneName());
			if (supplyValue.getSupplyValue() != NULL_VALUE) return supplyValue.getSupplyValue();
			//case no actual value and no children
			if (currentNode.getChildren().size() == 0) return NULL_VALUE;
			int currentValue = 0,childValue = 0;
			for (ZoneNode childNode : currentNode.getChildren())
			{
				childValue = fillDateValues(dateValue, childNode);
				if (childValue == NULL_VALUE) return NULL_VALUE;
				currentValue += childValue;
			}
			dateValue.get(currentNode.getZoneName()).setSupplyValue(currentValue);
			return currentValue;
		}
	}

	/**
	 * this method creates all tree nodes for the hierarchy tree but still without the connections (edges)
	 */
	protected void fillZoneNodeMap() {
		for(SupplyZone s : supplyZoneLines){
			String zoneName = s.getZoneName();
			zoneNodeMap.put(zoneName, new ZoneNode(zoneName));
		}
	}

	/**
	 * this method takes all the tree nodes and builds the connection (edges) between them according to the supply zones file. 
	 */
	protected void buildHirarchyTree() {
		for(SupplyZone s : supplyZoneLines){
			ZoneNode currentZone = zoneNodeMap.get(s.getZoneName());
			ZoneNode parentZone = zoneNodeMap.get(s.getParentZoneName());
			if(parentZone == null){
				zonesTreeRoot = currentZone;
			}
			currentZone.setParent(parentZone);
			if (parentZone != null) parentZone.addChild(currentZone); 
		}

	}

}
