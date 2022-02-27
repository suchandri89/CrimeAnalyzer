package com.ljmu.pancrime;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class PandemicDataProcessor implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public void processOnlineOfflineCrime(DataFrameReader dataFrameReader) {
		
		//iterating for 2 years
		for(Map.Entry<String, String> entry:Constants.YEAR_MAP.entrySet()) {
			
			if (entry.getKey().equalsIgnoreCase("2020")  || entry.getKey().equalsIgnoreCase("2021") ) {
		
			Dataset<Row> dataCut5 = dataFrameReader.option(Constants.HEAD,Constants.T)
	  				.csv(Constants.PATH3 + entry.getKey() + Constants.PATH2 + entry.getKey() + Constants.V4);
	
			//records containing major crime types of this report
			Dataset<Row> onlineCount = dataCut5.where(dataCut5.col(Constants.ARR_OFF).contains(Constants.TYPE_F1)//online
										.or(dataCut5.col(Constants.ARR_OFF).contains(Constants.TYPE_T3)))//online
							    		.groupBy(Constants.ARR_MON).count()
							    		.withColumnRenamed("count", Constants.ARR_ONLINE_CNT)
							    		.withColumn(Constants.ARR_YEAR, functions.lit(entry.getKey()))
							    		;
			
			Dataset<Row> finalOnlineCount = onlineCount.select(
											functions.concat(onlineCount.col(Constants.ARR_MON),
											functions.lit("-"),
											onlineCount.col(Constants.ARR_YEAR)).as(Constants.ARR_MON + "-" + Constants.ARR_YEAR),
											onlineCount.col(Constants.ARR_ONLINE_CNT)
											);
			
			finalOnlineCount.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
									.save(Constants.PATH3 + entry.getKey() + "/" + Constants.ARR_ONLINE_CNT);
			
			Dataset<Row> offlineCount = dataCut5.where(dataCut5.col(Constants.ARR_OFF).notEqual(Constants.TYPE_F1)
										.or(dataCut5.col(Constants.ARR_OFF).notEqual(Constants.TYPE_T3)))
							    		.groupBy(Constants.ARR_MON).count()
							    		.withColumnRenamed("count", Constants.ARR_OFFLINE_CNT)
							    		.withColumn(Constants.ARR_YEAR, functions.lit(entry.getKey()))
							    		;
			
			Dataset<Row> finalOfflineCount = offlineCount.select(
											functions.concat(offlineCount.col(Constants.ARR_MON),
											functions.lit("-"),
											offlineCount.col(Constants.ARR_YEAR)).as(Constants.ARR_MON + "-" + Constants.ARR_YEAR),
											offlineCount.col(Constants.ARR_OFFLINE_CNT)
											);
				
			finalOfflineCount.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
									.save(Constants.PATH3 + entry.getKey() + "/" + Constants.ARR_OFFLINE_CNT);
			
			}
		
		}
		
	}

	public void processBoroughWiseCrime(DataFrameReader dataFrameReader) {
		
		//iterating for 2 years
		for(Map.Entry<String, String> entry:Constants.YEAR_MAP.entrySet()) {
			
			if (entry.getKey().equalsIgnoreCase("2020")  || entry.getKey().equalsIgnoreCase("2021") ) {
				
				Dataset<Row> dataCut9 = dataFrameReader.option(Constants.HEAD,Constants.T)
		  				.csv(Constants.PATH3 + entry.getKey() + Constants.PATH2 + entry.getKey() + Constants.V4);
				
				
				Dataset<Row> boroughCrimeDataCut = dataCut9.groupBy(Constants.ARR_MON, Constants.ARR_OFF, Constants.ARR_BOR_STR).count()
			    								  .withColumnRenamed("count", Constants.BOR_CRM_CNT)
			    								  .withColumn(Constants.ARR_YEAR, functions.lit(entry.getKey()));
				
				Dataset<Row> finalboroughCrimeDataCut = boroughCrimeDataCut.select(
														functions.concat(boroughCrimeDataCut.col(Constants.ARR_MON),
														functions.lit("-"),
														boroughCrimeDataCut.col(Constants.ARR_YEAR)).as(Constants.ARR_MON + "-" + Constants.ARR_YEAR),
														boroughCrimeDataCut.col(Constants.BOR_CRM_CNT),
														boroughCrimeDataCut.col(Constants.ARR_OFF),
														boroughCrimeDataCut.col(Constants.ARR_BOR_STR)
														);
				
				finalboroughCrimeDataCut.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
												.save(Constants.PATH3 + entry.getKey() + "/" + Constants.BOR_CRM_CNT);
			}
			
		}
		
	}
	
	public void processGenderWiseCrime(DataFrameReader dataFrameReader) {
		
		//iterating for 2 years
		for(Map.Entry<String, String> entry:Constants.YEAR_MAP.entrySet()) {
			
			if (entry.getKey().equalsIgnoreCase("2020")  || entry.getKey().equalsIgnoreCase("2021") ) {
				
				Dataset<Row> dataCut9 = dataFrameReader.option(Constants.HEAD,Constants.T)
		  				.csv(Constants.PATH3 + entry.getKey() + Constants.PATH2 + entry.getKey() + Constants.V4);
				
				
				Dataset<Row> crimeDataCut = dataCut9.groupBy(Constants.ARR_MON, Constants.ARR_OFF, Constants.ARR_GEN).count()
				    								 .withColumnRenamed("count", Constants.GEN_CRM_CNT)
				    								 .withColumn(Constants.ARR_YEAR, functions.lit(entry.getKey()));
				
				Dataset<Row> finalCrimeDataCut = crimeDataCut.select(
															 functions.concat(crimeDataCut.col(Constants.ARR_MON),
															 functions.lit("-"),
															 crimeDataCut.col(Constants.ARR_YEAR)).as(Constants.ARR_MON + "-" + Constants.ARR_YEAR),
															 crimeDataCut.col(Constants.GEN_CRM_CNT),
															 crimeDataCut.col(Constants.ARR_OFF),
															 crimeDataCut.col(Constants.ARR_GEN)
															 );
				
				finalCrimeDataCut.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
										 .save(Constants.PATH3 + entry.getKey() + "/" + Constants.GEN_CRM_CNT);
			}
			
		}
		
	}
	
	public void processAgeGroupWiseCrime(DataFrameReader dataFrameReader) {
		
		//iterating for 2 years
		for(Map.Entry<String, String> entry:Constants.YEAR_MAP.entrySet()) {
			
			if (entry.getKey().equalsIgnoreCase("2020")  || entry.getKey().equalsIgnoreCase("2021") ) {
				
				Dataset<Row> dataCut9 = dataFrameReader.option(Constants.HEAD,Constants.T)
		  				.csv(Constants.PATH3 + entry.getKey() + Constants.PATH2 + entry.getKey() + Constants.V4);
				
				
				Dataset<Row> crimeDataCut = dataCut9.groupBy(Constants.ARR_MON, Constants.ARR_OFF, Constants.ARR_AGE_GRP).count()
			    								    .withColumnRenamed("count", Constants.AGE_CRM_CNT)
			    								    .withColumn(Constants.ARR_YEAR, functions.lit(entry.getKey()));
				
				Dataset<Row> finalCrimeDataCut = crimeDataCut.select(
															functions.concat(crimeDataCut.col(Constants.ARR_MON),
															functions.lit("-"),
															crimeDataCut.col(Constants.ARR_YEAR)).as(Constants.ARR_MON + "-" + Constants.ARR_YEAR),
															crimeDataCut.col(Constants.AGE_CRM_CNT),
															crimeDataCut.col(Constants.ARR_OFF),
															crimeDataCut.col(Constants.ARR_AGE_GRP)
															);
				
				finalCrimeDataCut.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
												.save(Constants.PATH3 + entry.getKey() + "/" + Constants.AGE_CRM_CNT);
			}
			
		}
		
	}

	
	
	




}
