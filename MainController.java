package com.ljmu.pancrime;

import java.io.IOException;
import java.text.ParseException;

import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.Level;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;

/*
 * The main controller class for Pandemic Crime Classifier Application
 */
public class MainController {
  
  public static void main(String[] args) throws InterruptedException, IOException, NumberFormatException, ParseException {

    //Setting logging level to Warning
    Configurator.setLevel("", Level.ERROR);
    
    // For SparkSQL creating a SparkSession object	
	SparkSession session = SparkSession.builder().appName("POST2").master("local").getOrCreate() ;
	
	// Dataframereader object is required to read data from an external file	
    DataFrameReader dataFrameReader = session.read(); //V0
    
    AnnualDataRefiner adr = new AnnualDataRefiner();
    adr.cleanseData(session,dataFrameReader);   //V4
    
    HistoricDataProcessor hdp = new HistoricDataProcessor(); //V4 for 2015 to 2019
    hdp.processAnnualCrime(dataFrameReader); //crime, age, bor, gen monthly count for each year from 2015 to 2019
    
    PandemicDataProcessor pdp = new PandemicDataProcessor(); 
    
    //find online and offline crimes
    hdp.processOnlineOfflineCrime(dataFrameReader); // ip: hist/crime op: hist/off,on
    pdp.processOnlineOfflineCrime(dataFrameReader); // V4 for 2020 & 2021 (ip) | (op) yr/off,on
    
    //find area/borough wise crimes    
    hdp.processBoroughWiseCrime(dataFrameReader);
    pdp.processBoroughWiseCrime(dataFrameReader);
    
    //find age-group wise crimes
    hdp.processAgeGroupWiseCrime(dataFrameReader);
    pdp.processAgeGroupWiseCrime(dataFrameReader);
    
    //find criminal's gender wise crimes
    hdp.processGenderWiseCrime(dataFrameReader);
    pdp.processGenderWiseCrime(dataFrameReader);
    
    
       
    
    
    

	session.close();	 
    
      
  }

  
}
