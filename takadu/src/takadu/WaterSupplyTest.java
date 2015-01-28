package takadu;

import static org.junit.Assert.*;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

import takadu.WaterSupply.fillDateValuesJob;

public class WaterSupplyTest {

	@Test
	public void testAddToSupplyZoneLines() {
		WaterSupply waterSupply = new WaterSupply();
		//test on empty supplyZone
		waterSupply.supplyZoneLines = new ArrayList<SupplyZone>();
		waterSupply.addToSupplyZoneLines("Tel aviv,Gush Dan");
		assertEquals(waterSupply.supplyZoneLines.size(),1);
		assertEquals(waterSupply.supplyZoneLines.get(0).getZoneName(), "Tel aviv");
		assertEquals(waterSupply.supplyZoneLines.get(0).getParentZoneName(), "Gush Dan");
		
		//test adding when not empty
		waterSupply.addToSupplyZoneLines("Ramat Gan,Gush Dan");
		assertEquals(waterSupply.supplyZoneLines.size(),2);
		assertEquals(waterSupply.supplyZoneLines.get(0).getZoneName(), "Tel aviv");
		assertEquals(waterSupply.supplyZoneLines.get(0).getParentZoneName(), "Gush Dan");
		assertEquals(waterSupply.supplyZoneLines.get(1).getZoneName(), "Ramat Gan");
		assertEquals(waterSupply.supplyZoneLines.get(1).getParentZoneName(), "Gush Dan");
		
		//test adding null parent
		waterSupply.addToSupplyZoneLines("Gush Dan,");
		assertEquals(waterSupply.supplyZoneLines.size(),3);
		assertEquals(waterSupply.supplyZoneLines.get(2).getZoneName(), "Gush Dan");
		assertEquals(waterSupply.supplyZoneLines.get(2).getParentZoneName(), null);
	}

	@Test
	public void testFillMissingValues() {
		WaterSupply waterSupply = new WaterSupply();
		waterSupply.parseZones(new File("./res/testZones3.csv")); //fills the supplyZoneLines list.
		waterSupply.fillZoneNodeMap();
		waterSupply.buildHirarchyTree();
		waterSupply.parseValues(new File("./res/testValues3.csv"));
		waterSupply.fillMissingValues();
		testValues3Verifications(waterSupply);
	}

	@Test
	public void testFillDateJobValues() {
		WaterSupply waterSupply = new WaterSupply();
		waterSupply.parseZones(new File("./res/testZones3.csv")); //fills the supplyZoneLines list.
		waterSupply.fillZoneNodeMap();
		waterSupply.buildHirarchyTree();
		waterSupply.parseValues(new File("./res/testValues3.csv"));
		//all tests are based on a tree that contains one zone with one sub-zone and one with 2 sub-zones.
		fillDateValuesJob fillJob = new fillDateValuesJob(waterSupply.datesValuesMap.get("01/01/14"), waterSupply.zonesTreeRoot);
		Thread currentThread;
		ArrayList<Thread> threads = new ArrayList<Thread>();
		currentThread = new Thread(fillJob);
		currentThread.start();
		threads.add(currentThread);
		fillJob = new fillDateValuesJob(waterSupply.datesValuesMap.get("02/01/14"), waterSupply.zonesTreeRoot);
		currentThread = new Thread(fillJob);
		currentThread.start();
		threads.add(currentThread);
		fillJob = new fillDateValuesJob(waterSupply.datesValuesMap.get("03/01/14"), waterSupply.zonesTreeRoot);
		currentThread = new Thread(fillJob);
		currentThread.start();
		threads.add(currentThread);
		fillJob = new fillDateValuesJob(waterSupply.datesValuesMap.get("04/01/14"), waterSupply.zonesTreeRoot);
		currentThread = new Thread(fillJob);
		currentThread.start();
		threads.add(currentThread);
		for (Thread thread : threads) {
			try {
				thread.join(WaterSupply.THREAD_TIMEOUT);
			} catch (InterruptedException e) {
				System.err.println("one of the working threads has been interrupted");
				assertTrue(false);
			}
			assertTrue(!thread.isAlive());
		}
		
		testValues3Verifications(waterSupply);
	}

	private void testValues3Verifications(WaterSupply waterSupply) {
		// 01/01/14 - case missing values in leaves.
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Tel Aviv").getSupplyValue(),  WaterSupply.NULL_VALUE);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Tel Aviv").isActual(),  false);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Ramat Gan").getSupplyValue(),  WaterSupply.NULL_VALUE);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Ramat Hen").getSupplyValue(),  WaterSupply.NULL_VALUE);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Jaffa").getSupplyValue(),  WaterSupply.NULL_VALUE);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Jaffa").isActual(),  false);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Gush Dan").getSupplyValue(),  WaterSupply.NULL_VALUE);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Ramat Aviv").getSupplyValue(),  10);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Ramat Aviv").isActual(),  true);
		
		// 02/01/14 - case missing values in middle level but in leaves no values missing
		assertEquals(waterSupply.datesValuesMap.get("02/01/14").get("Tel Aviv").getSupplyValue(), 8);
		assertEquals(waterSupply.datesValuesMap.get("02/01/14").get("Tel Aviv").isActual(),  false);
		assertEquals(waterSupply.datesValuesMap.get("02/01/14").get("Ramat Gan").getSupplyValue(),  6);
		assertEquals(waterSupply.datesValuesMap.get("02/01/14").get("Ramat Gan").isActual(),  false);
		assertEquals(waterSupply.datesValuesMap.get("02/01/14").get("Ramat Hen").getSupplyValue(),  6);
		assertEquals(waterSupply.datesValuesMap.get("02/01/14").get("Jaffa").getSupplyValue(), 5);
		assertEquals(waterSupply.datesValuesMap.get("02/01/14").get("Jaffa").isActual(),  true);
		assertEquals(waterSupply.datesValuesMap.get("02/01/14").get("Gush Dan").getSupplyValue(),  14);
		assertEquals(waterSupply.datesValuesMap.get("02/01/14").get("Ramat Aviv").getSupplyValue(),  3);
		assertEquals(waterSupply.datesValuesMap.get("02/01/14").get("Ramat Aviv").isActual(),  true);
		
		//03/01/14 - case only missing values in root, checking no values are over written
		assertEquals(waterSupply.datesValuesMap.get("03/01/14").get("Tel Aviv").getSupplyValue(), 11);
		assertEquals(waterSupply.datesValuesMap.get("03/01/14").get("Tel Aviv").isActual(),  true);
		assertEquals(waterSupply.datesValuesMap.get("03/01/14").get("Ramat Gan").getSupplyValue(),  10);
		assertEquals(waterSupply.datesValuesMap.get("03/01/14").get("Ramat Gan").isActual(),  true);
		assertEquals(waterSupply.datesValuesMap.get("03/01/14").get("Ramat Hen").getSupplyValue(),  6);
		assertEquals(waterSupply.datesValuesMap.get("03/01/14").get("Jaffa").getSupplyValue(), 5);
		assertEquals(waterSupply.datesValuesMap.get("03/01/14").get("Jaffa").isActual(),  true);
		assertEquals(waterSupply.datesValuesMap.get("03/01/14").get("Gush Dan").getSupplyValue(),  21);
		assertEquals(waterSupply.datesValuesMap.get("03/01/14").get("Ramat Aviv").getSupplyValue(),  3);
		assertEquals(waterSupply.datesValuesMap.get("03/01/14").get("Ramat Aviv").isActual(),  true);
		
		//04/01/14 - case no missing values in root, checking no values are over written
		assertEquals(waterSupply.datesValuesMap.get("04/01/14").get("Tel Aviv").getSupplyValue(), 7);
		assertEquals(waterSupply.datesValuesMap.get("04/01/14").get("Tel Aviv").isActual(),  true);
		assertEquals(waterSupply.datesValuesMap.get("04/01/14").get("Ramat Gan").getSupplyValue(),  10);
		assertEquals(waterSupply.datesValuesMap.get("04/01/14").get("Ramat Gan").isActual(),  true);
		assertEquals(waterSupply.datesValuesMap.get("04/01/14").get("Ramat Hen").getSupplyValue(),  6);
		assertEquals(waterSupply.datesValuesMap.get("04/01/14").get("Jaffa").getSupplyValue(), 5);
		assertEquals(waterSupply.datesValuesMap.get("04/01/14").get("Jaffa").isActual(),  true);
		assertEquals(waterSupply.datesValuesMap.get("04/01/14").get("Gush Dan").getSupplyValue(),  13);
		assertEquals(waterSupply.datesValuesMap.get("04/01/14").get("Gush Dan").isActual(),  true);
		assertEquals(waterSupply.datesValuesMap.get("04/01/14").get("Ramat Aviv").getSupplyValue(),  3);
		assertEquals(waterSupply.datesValuesMap.get("04/01/14").get("Ramat Aviv").isActual(),  true);
	}

	@Test
	public void testBuildHirarchyTree() {
		WaterSupply waterSupply = new WaterSupply();
		waterSupply.addToSupplyZoneLines("Givataim,Gush Dan");
		waterSupply.addToSupplyZoneLines("Ramat Hen,Ramat Gan");
		waterSupply.addToSupplyZoneLines("Ramat Gan,Gush Dan");
		waterSupply.addToSupplyZoneLines("Gush Dan,");
		waterSupply.fillZoneNodeMap();
		
		waterSupply.buildHirarchyTree();
		
		assertEquals(waterSupply.zoneNodeMap.size(), 4);
		
		assertEquals(waterSupply.zoneNodeMap.get("Givataim").getZoneName(), "Givataim");
		assertEquals(waterSupply.zoneNodeMap.get("Givataim").getParent().getZoneName(), "Gush Dan");
		assertEquals(waterSupply.zoneNodeMap.get("Givataim").getChildren(), new ArrayList<ZoneNode>());
		
		assertEquals(waterSupply.zoneNodeMap.get("Ramat Hen").getZoneName(), "Ramat Hen");
		assertEquals(waterSupply.zoneNodeMap.get("Ramat Hen").getParent().getZoneName(), "Ramat Gan");
		assertEquals(waterSupply.zoneNodeMap.get("Ramat Hen").getChildren(), new ArrayList<ZoneNode>());
		
		assertEquals(waterSupply.zoneNodeMap.get("Gush Dan").getZoneName(), "Gush Dan");
		assertEquals(waterSupply.zoneNodeMap.get("Gush Dan").getParent(), null);
		assertEquals(waterSupply.zoneNodeMap.get("Gush Dan").getChildren().size(), 2);
		assertEquals(waterSupply.zoneNodeMap.get("Gush Dan").getChildren().get(0).getZoneName(), "Givataim");
		assertEquals(waterSupply.zoneNodeMap.get("Gush Dan").getChildren().get(1).getZoneName(), "Ramat Gan");
		
		assertEquals(waterSupply.zoneNodeMap.get("Ramat Gan").getZoneName(), "Ramat Gan");
		assertEquals(waterSupply.zoneNodeMap.get("Ramat Gan").getParent().getZoneName(), "Gush Dan");
		assertEquals(waterSupply.zoneNodeMap.get("Ramat Gan").getChildren().size(), 1);
		assertEquals(waterSupply.zoneNodeMap.get("Ramat Gan").getChildren().get(0).getZoneName(), "Ramat Hen");
	}

	@Test
	public void testFillZoneNodeMap() {
		WaterSupply waterSupply = new WaterSupply();
		waterSupply.addToSupplyZoneLines("Givataim,Gush Dan");
		waterSupply.addToSupplyZoneLines("Ramat Hen,Ramat Gan");
		waterSupply.addToSupplyZoneLines("Ramat Gan,Gush Dan");
		waterSupply.addToSupplyZoneLines("Gush Dan,");
		
		waterSupply.fillZoneNodeMap();
		assertEquals(waterSupply.zoneNodeMap.size(), 4);
		assertEquals(waterSupply.zoneNodeMap.get("Givataim").getZoneName(), "Givataim");
		assertEquals(waterSupply.zoneNodeMap.get("Givataim").getParent(), null);
		assertEquals(waterSupply.zoneNodeMap.get("Givataim").getChildren(), new ArrayList<ZoneNode>());
		
		assertEquals(waterSupply.zoneNodeMap.get("Ramat Hen").getZoneName(), "Ramat Hen");
		assertEquals(waterSupply.zoneNodeMap.get("Ramat Hen").getParent(), null);
		assertEquals(waterSupply.zoneNodeMap.get("Ramat Hen").getChildren(), new ArrayList<ZoneNode>());
		
		assertEquals(waterSupply.zoneNodeMap.get("Gush Dan").getZoneName(), "Gush Dan");
		assertEquals(waterSupply.zoneNodeMap.get("Gush Dan").getParent(), null);
		assertEquals(waterSupply.zoneNodeMap.get("Gush Dan").getChildren(),  new ArrayList<ZoneNode>());
		
		assertEquals(waterSupply.zoneNodeMap.get("Ramat Gan").getZoneName(), "Ramat Gan");
		assertEquals(waterSupply.zoneNodeMap.get("Ramat Gan").getParent(), null);
		assertEquals(waterSupply.zoneNodeMap.get("Ramat Gan").getChildren(), new ArrayList<ZoneNode>());
	}

	@Test
	public void testParseZones() {
		WaterSupply waterSupply = new WaterSupply();
		
		//testing empty file
		waterSupply.parseZones(new File("./res/testZones1.csv"));
		assertEquals(waterSupply.supplyZoneLines.size(),0);
		
		//testing non empty file
		waterSupply.supplyZoneLines = new ArrayList<SupplyZone>();
		waterSupply.parseZones(new File("./res/testZones2.csv"));
		assertEquals(waterSupply.supplyZoneLines.size(),3);
		assertEquals(waterSupply.supplyZoneLines.get(0).getZoneName(), "Gush Dan");
		assertEquals(waterSupply.supplyZoneLines.get(0).getParentZoneName(), null);
		assertEquals(waterSupply.supplyZoneLines.get(1).getZoneName(), "Tel Aviv");
		assertEquals(waterSupply.supplyZoneLines.get(1).getParentZoneName(), "Gush Dan");
		assertEquals(waterSupply.supplyZoneLines.get(2).getZoneName(), "Ramat Gan");
		assertEquals(waterSupply.supplyZoneLines.get(2).getParentZoneName(), "Gush Dan");
	}

	@Test
	public void testParseValues() throws ParseException {
		WaterSupply waterSupply = new WaterSupply();
		waterSupply.addToSupplyZoneLines("Gush Dan,");
		waterSupply.addToSupplyZoneLines("Givataim,Gush Dan");
		waterSupply.addToSupplyZoneLines("Ramat Gan,Gush Dan");
		waterSupply.addToSupplyZoneLines("Ramat Hen,Ramat Gan");
		waterSupply.addToSupplyZoneLines("Ramat Aviv,Tel Aviv");
		waterSupply.addToSupplyZoneLines("Tel Aviv,Gush Dan");
		
		//test on empty file
		waterSupply.datesValuesMap = new HashMap<String, HashMap<String, SupplyValue>>();
		waterSupply.parseValues(new File("./res/testValues1.csv"));
		assertEquals(waterSupply.datesValuesMap.size() , 0);
		
		//testing non empty file
		waterSupply.datesValuesMap = new HashMap<String, HashMap<String, SupplyValue>>();
		waterSupply.parseValues(new File("./res/testValues2.csv"));
		assertEquals(waterSupply.datesValuesMap.size() , 3);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").size(), 6);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Givataim").getSupplyValue(),  10);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Givataim").isActual(),  true);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Ramat Hen").getSupplyValue(),  6);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Ramat Hen").isActual(),  true);
		
		assertEquals(waterSupply.datesValuesMap.get("04/01/14").size(), 6);
		assertEquals(waterSupply.datesValuesMap.get("04/01/14").get("Ramat Aviv").getSupplyValue(),  2);
		assertEquals(waterSupply.datesValuesMap.get("04/01/14").get("Ramat Aviv").isActual(),  true);
	
		assertEquals(waterSupply.datesValuesMap.get("07/01/14").size(), 6);
		assertEquals(waterSupply.datesValuesMap.get("07/01/14").get("Ramat Hen").getSupplyValue(),  6);
		assertEquals(waterSupply.datesValuesMap.get("07/01/14").get("Ramat Hen").isActual(),  true);
		assertEquals(waterSupply.datesValuesMap.get("07/01/14").get("Givataim").getSupplyValue(),  WaterSupply.NULL_VALUE);
		assertEquals(waterSupply.datesValuesMap.get("07/01/14").get("Givataim").isActual(),  false);
		assertEquals(waterSupply.datesValuesMap.get("07/01/14").get("Ramat Aviv").getSupplyValue(),  WaterSupply.NULL_VALUE);
		assertEquals(waterSupply.datesValuesMap.get("07/01/14").get("Ramat Aviv").isActual(),  false);	
		
	}

	@Test
	public void testAddToSupplyValues() throws ParseException {
		WaterSupply waterSupply = new WaterSupply();
		
		waterSupply.addToSupplyZoneLines("Givataim,Gush Dan");
		waterSupply.addToSupplyZoneLines("Ramat Hen,Ramat Gan");
		waterSupply.addToSupplyZoneLines("Ramat Gan,Gush Dan");
		waterSupply.addToSupplyZoneLines("Gush Dan,");
		
		//test on empty datesMap
		waterSupply.datesValuesMap = new HashMap<String, HashMap<String, SupplyValue>>();
		waterSupply.addToSupplyValues("Givataim,01/01/14,10");
		assertEquals(waterSupply.datesValuesMap.size(), 1);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").size(), 4);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Givataim").getSupplyValue(),  10);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Givataim").isActual(),  true);
		
		//test adding line with the same date key to the map
		waterSupply.addToSupplyValues("Ramat Hen,01/01/14,6");
		assertEquals(waterSupply.datesValuesMap.size(), 1);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").size(), 4);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Ramat Hen").getSupplyValue(),  6);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Ramat Hen").isActual(),  true);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Ramat Gan").getSupplyValue(),  WaterSupply.NULL_VALUE);
		assertEquals(waterSupply.datesValuesMap.get("01/01/14").get("Ramat Gan").isActual(),  false);
		
		//test adding another date key to the map
		waterSupply.addToSupplyValues("Givataim,02/01/14,11");
		assertEquals(waterSupply.datesValuesMap.size(), 2);
		assertEquals(waterSupply.datesValuesMap.get("02/01/14").size(), 4);
		assertEquals(waterSupply.datesValuesMap.get("02/01/14").get("Givataim").getSupplyValue(),  11);
		assertEquals(waterSupply.datesValuesMap.get("02/01/14").get("Givataim").isActual(),  true);
		assertEquals(waterSupply.datesValuesMap.get("02/01/14").get("Ramat Gan").getSupplyValue(),  WaterSupply.NULL_VALUE);
		assertEquals(waterSupply.datesValuesMap.get("02/01/14").get("Ramat Gan").isActual(),  false);
		
	}

}
