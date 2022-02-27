package com.ljmu.pancrime;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class HistoricDataProcessor implements Serializable{

/**
	 * 
	 */
	private static final long serialVersionUID = 1L;





	public void processAnnualCrime(DataFrameReader dataFrameReader) {
		
		//iterating for 5 years
		for(Map.Entry<String, String> entry:Constants.YEAR_MAP.entrySet()) {
			
			if (!entry.getKey().equalsIgnoreCase("2020") && !entry.getKey().equalsIgnoreCase("2021") ) {
			
				//process monthly crime data
				Dataset<Row> dataCut6 = dataFrameReader.option(Constants.HEAD,Constants.T)
		  								.csv(Constants.PATH3 + entry.getKey() + Constants.PATH2 + entry.getKey() + Constants.V4);
				
				Dataset<Row> monthlyCrimeCount = dataCut6.groupBy(Constants.ARR_OFF,Constants.ARR_MON).count()
										    		.withColumnRenamed("count", Constants.ARR_CRM_CNT)
										    		.withColumn(Constants.ARR_YEAR, functions.lit(entry.getKey()));
				
				Dataset<Row> finalMonthlyCrimeCount = monthlyCrimeCount.select(
														functions.concat(monthlyCrimeCount.col(Constants.ARR_MON),
														functions.lit("-"),
														monthlyCrimeCount.col(Constants.ARR_YEAR)).as(Constants.ARR_MON + "-" + Constants.ARR_YEAR),
														monthlyCrimeCount.col(Constants.ARR_OFF),
														monthlyCrimeCount.col(Constants.ARR_MON),
														monthlyCrimeCount.col(Constants.ARR_CRM_CNT)
														);
				
				finalMonthlyCrimeCount.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
												.save(Constants.PATH3 + entry.getKey() + Constants.PATH5);
				
				
				//process monthly borough wise crime data
				Dataset<Row> monthlyBoroughWiseCrimeCount = dataCut6.groupBy(Constants.ARR_OFF,Constants.ARR_MON,Constants.ARR_BOR_STR).count()
														    		.withColumnRenamed("count", Constants.BOR_CRM_CNT)
														    		.withColumn(Constants.ARR_YEAR, functions.lit(entry.getKey()));
				
				Dataset<Row> finalMonthlyBoroughWiseCrimeCount = monthlyBoroughWiseCrimeCount.select(
																 functions.concat(monthlyBoroughWiseCrimeCount.col(Constants.ARR_MON),
																 functions.lit("-"),
																 monthlyBoroughWiseCrimeCount.col(Constants.ARR_YEAR)).as(Constants.ARR_MON + "-" + Constants.ARR_YEAR),
																 monthlyBoroughWiseCrimeCount.col(Constants.ARR_OFF),
																 monthlyBoroughWiseCrimeCount.col(Constants.ARR_MON),
																 monthlyBoroughWiseCrimeCount.col(Constants.ARR_BOR_STR),
																 monthlyBoroughWiseCrimeCount.col(Constants.BOR_CRM_CNT)
																 );

				finalMonthlyBoroughWiseCrimeCount.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
												 .save(Constants.PATH3 + entry.getKey() + "/" + Constants.BOR_CRM_CNT);
				
				
				
				//process monthly age-group wise crime data
				Dataset<Row> monthlyAgeGroupWiseCrimeCount = dataCut6.groupBy(Constants.ARR_OFF,Constants.ARR_MON,Constants.ARR_AGE_GRP).count()
														    		 .withColumnRenamed("count", Constants.AGE_CRM_CNT)
														    		 .withColumn(Constants.ARR_YEAR, functions.lit(entry.getKey()));
				
				Dataset<Row> finalMonthlyAgeGroupWiseCrimeCount = monthlyAgeGroupWiseCrimeCount.select(
																 functions.concat(monthlyAgeGroupWiseCrimeCount.col(Constants.ARR_MON),
																 functions.lit("-"),
																 monthlyAgeGroupWiseCrimeCount.col(Constants.ARR_YEAR)).as(Constants.ARR_MON + "-" + Constants.ARR_YEAR),
																 monthlyAgeGroupWiseCrimeCount.col(Constants.ARR_OFF),
																 monthlyAgeGroupWiseCrimeCount.col(Constants.ARR_MON),
																 monthlyAgeGroupWiseCrimeCount.col(Constants.ARR_AGE_GRP),
																 monthlyAgeGroupWiseCrimeCount.col(Constants.AGE_CRM_CNT)
																 );

				finalMonthlyAgeGroupWiseCrimeCount.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
												 .save(Constants.PATH3 + entry.getKey() + "/" + Constants.AGE_CRM_CNT);
				
				
				
				//process monthly gender wise crime data
				Dataset<Row> monthlyGenderWiseCrimeCount = dataCut6.groupBy(Constants.ARR_OFF,Constants.ARR_MON,Constants.ARR_GEN).count()
														    	   .withColumnRenamed("count", Constants.GEN_CRM_CNT)
														    	   .withColumn(Constants.ARR_YEAR, functions.lit(entry.getKey()));
				
				Dataset<Row> finalMonthlyGenderWiseCrimeCount = monthlyGenderWiseCrimeCount.select(
																 functions.concat(monthlyGenderWiseCrimeCount.col(Constants.ARR_MON),
																 functions.lit("-"),
																 monthlyGenderWiseCrimeCount.col(Constants.ARR_YEAR)).as(Constants.ARR_MON + "-" + Constants.ARR_YEAR),
																 monthlyGenderWiseCrimeCount.col(Constants.ARR_OFF),
																 monthlyGenderWiseCrimeCount.col(Constants.ARR_MON),
																 monthlyGenderWiseCrimeCount.col(Constants.ARR_GEN),
																 monthlyGenderWiseCrimeCount.col(Constants.GEN_CRM_CNT)
																 );

				finalMonthlyGenderWiseCrimeCount.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
												.save(Constants.PATH3 + entry.getKey() + "/" + Constants.GEN_CRM_CNT);


			}
			
		}
				
	}




	
	private void monthlyMovingCrimeAverage(DataFrameReader dataFrameReader) {
		
		Dataset<Row> dataCut2015 = dataFrameReader.option(Constants.HEAD,Constants.T)
									.csv(Constants.PATH3 + "2015" + Constants.PATH5);
		
		Dataset<Row> dataCut2016 = dataFrameReader.option(Constants.HEAD,Constants.T)
									.csv(Constants.PATH3 + "2016" + Constants.PATH5);
		
		Dataset<Row> dataCut2017 = dataFrameReader.option(Constants.HEAD,Constants.T)
									.csv(Constants.PATH3 + "2017" + Constants.PATH5);
		
		Dataset<Row> dataCut2018 = dataFrameReader.option(Constants.HEAD,Constants.T)
									.csv(Constants.PATH3 + "2018" + Constants.PATH5);
		
		Dataset<Row> dataCut2019 = dataFrameReader.option(Constants.HEAD,Constants.T)
									.csv(Constants.PATH3 + "2019" + Constants.PATH5);
		
		Dataset<Row> unionCrimeData = dataCut2015.union(dataCut2016).union(dataCut2017).union(dataCut2018).union(dataCut2019);
		
		
		unionCrimeData.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
										.save(Constants.PATH3 + "Historic" + Constants.PATH5);
		
		
		
		Dataset<Row> dataCut7 = dataFrameReader.option(Constants.HEAD,Constants.T)
				.csv(Constants.PATH3 + Constants.PATH6 + Constants.PATH5);
		
		
		Dataset<Row> dataCutCrimeSum = dataCut7.groupBy(dataCut7.col(Constants.ARR_MON),
						 dataCut7.col(Constants.ARR_OFF)).agg(functions.sum(dataCut7.col(Constants.ARR_CRM_CNT)).as(Constants.CRM_SUM));
		
		Dataset<Row> dataCutCrimeAvg = dataCutCrimeSum.select(dataCutCrimeSum.col(Constants.ARR_MON),
															 dataCutCrimeSum.col(Constants.ARR_OFF),
															 dataCutCrimeSum.col(Constants.CRM_SUM),
															 dataCutCrimeSum.col(Constants.CRM_SUM).$div(5).as(Constants.CRM_AVG)
										);
		
		dataCutCrimeAvg.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
								.save(Constants.PATH3 + Constants.PATH6 + Constants.PATH7);
		
		
	}
	
	public void processOnlineOfflineCrime(DataFrameReader dataFrameReader) {
		
		this.monthlyMovingCrimeAverage(dataFrameReader);
		
		Dataset<Row> dataCut8 = dataFrameReader.option(Constants.HEAD,Constants.T)
  											   .csv(Constants.PATH3 + Constants.PATH6 + Constants.PATH7);

		Dataset<Row> dataCut10 = dataCut8.where(dataCut8.col(Constants.ARR_OFF).contains(Constants.TYPE_F1)
								.or(dataCut8.col(Constants.ARR_OFF).contains(Constants.TYPE_T3)));

		Dataset<Row> onlineCount = dataCut10.groupBy(dataCut10.col(Constants.ARR_MON))
					.agg(functions.sum(dataCut10.col(Constants.CRM_AVG)).as(Constants.ARR_ONLINE_CNT));

		/*
		Dataset<Row> onlineCount = dataCut8.where(dataCut8.col(Constants.ARR_OFF).contains(Constants.TYPE_F1)//online
									.or(dataCut8.col(Constants.ARR_OFF).contains(Constants.TYPE_T3)))//online
						    		.groupBy(Constants.ARR_MON).count()
						    		.withColumnRenamed("count", Constants.ARR_ONLINE_CNT)
						    		.withColumn(Constants.ARR_YEAR, functions.lit("2019"))
						    		;
		*/
		Dataset<Row> finalOnlineCount = onlineCount.select(
										functions.concat(onlineCount.col(Constants.ARR_MON),
										functions.lit("-"),functions.lit("2019"))
										.as(Constants.ARR_MON + "-" + Constants.ARR_YEAR),
										onlineCount.col(Constants.ARR_ONLINE_CNT)
										);
		
		finalOnlineCount.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
								.save(Constants.PATH3 + Constants.PATH6 + "/" + Constants.ARR_ONLINE_CNT);
		
		Dataset<Row> dataCut9 = dataCut8.where(dataCut8.col(Constants.ARR_OFF).notEqual(Constants.TYPE_F1)
								.and(dataCut8.col(Constants.ARR_OFF).notEqual(Constants.TYPE_T3)));
		
		Dataset<Row> offlineCount = dataCut9.groupBy(dataCut9.col(Constants.ARR_MON))
									.agg(functions.sum(dataCut9.col(Constants.CRM_AVG)).as(Constants.ARR_OFFLINE_CNT));
				
		
		Dataset<Row> finalOfflineCount = offlineCount.select(
										functions.concat(offlineCount.col(Constants.ARR_MON),
										functions.lit("-"),functions.lit("2019"))
										.as(Constants.ARR_MON + "-" + Constants.ARR_YEAR),
										offlineCount.col(Constants.ARR_OFFLINE_CNT)
										);
			
		finalOfflineCount.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
								.save(Constants.PATH3 + Constants.PATH6 + "/" + Constants.ARR_OFFLINE_CNT);
			
		
	}





	public void processBoroughWiseCrime(DataFrameReader dataFrameReader) {
		
		this.monthlyBoroughWiseMovingCrimeAverage(dataFrameReader);
		
	}


	private void monthlyBoroughWiseMovingCrimeAverage(DataFrameReader dataFrameReader) {
		
		Dataset<Row> dataCut2015 = dataFrameReader.option(Constants.HEAD,Constants.T)
				.csv(Constants.PATH3 + "2015" + "/" + Constants.BOR_CRM_CNT);

		Dataset<Row> dataCut2016 = dataFrameReader.option(Constants.HEAD,Constants.T)
						.csv(Constants.PATH3 + "2016" + "/" + Constants.BOR_CRM_CNT);
		
		Dataset<Row> dataCut2017 = dataFrameReader.option(Constants.HEAD,Constants.T)
						.csv(Constants.PATH3 + "2017" + "/" + Constants.BOR_CRM_CNT);
		
		Dataset<Row> dataCut2018 = dataFrameReader.option(Constants.HEAD,Constants.T)
						.csv(Constants.PATH3 + "2018" + "/" + Constants.BOR_CRM_CNT);
		
		Dataset<Row> dataCut2019 = dataFrameReader.option(Constants.HEAD,Constants.T)
						.csv(Constants.PATH3 + "2019" + "/" + Constants.BOR_CRM_CNT);
		
		Dataset<Row> unionCrimeData = dataCut2015.union(dataCut2016).union(dataCut2017).union(dataCut2018).union(dataCut2019);
		
		
		unionCrimeData.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
							.save(Constants.PATH3 + "Historic/MONTHLY_" + Constants.BOR_CRM_CNT);
		
		
		
		Dataset<Row> dataCut7 = dataFrameReader.option(Constants.HEAD,Constants.T)
											   .csv(Constants.PATH3 + Constants.PATH6 + "/MONTHLY_" + Constants.BOR_CRM_CNT);
		
		
		Dataset<Row> dataCutCrimeSum = dataCut7.groupBy(dataCut7.col(Constants.ARR_MON),
														dataCut7.col(Constants.ARR_OFF),
														dataCut7.col(Constants.ARR_BOR_STR)
												).agg(functions.sum(dataCut7.col(Constants.BOR_CRM_CNT)).as(Constants.BOR_CRM_SUM))
												 .withColumn(Constants.ARR_YEAR, functions.lit("2019"));
		
		Dataset<Row> dataCutCrimeAvg = dataCutCrimeSum.select(functions.concat(dataCutCrimeSum.col(Constants.ARR_MON),
															  functions.lit("-"),
															  dataCutCrimeSum.col(Constants.ARR_YEAR)).as(Constants.ARR_MON + "-" + Constants.ARR_YEAR),
															  dataCutCrimeSum.col(Constants.ARR_OFF),
															  dataCutCrimeSum.col(Constants.ARR_BOR_STR),
															  dataCutCrimeSum.col(Constants.BOR_CRM_SUM),
															  dataCutCrimeSum.col(Constants.BOR_CRM_SUM).$div(5).as(Constants.BOR_CRM_AVG)
															);
		
		dataCutCrimeAvg.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
							   .save(Constants.PATH3 + Constants.PATH6 + "/" + Constants.BOR_CRM_CNT);
		
	}
	

	public void processAgeGroupWiseCrime(DataFrameReader dataFrameReader) {
		
		this.monthlyAgeGroupWiseMovingCrimeAverage(dataFrameReader);
		
	}
	
	private void monthlyAgeGroupWiseMovingCrimeAverage(DataFrameReader dataFrameReader) {
		
		Dataset<Row> dataCut2015 = dataFrameReader.option(Constants.HEAD,Constants.T)
				.csv(Constants.PATH3 + "2015" + "/" + Constants.AGE_CRM_CNT);

		Dataset<Row> dataCut2016 = dataFrameReader.option(Constants.HEAD,Constants.T)
						.csv(Constants.PATH3 + "2016" + "/" + Constants.AGE_CRM_CNT);
		
		Dataset<Row> dataCut2017 = dataFrameReader.option(Constants.HEAD,Constants.T)
						.csv(Constants.PATH3 + "2017" + "/" + Constants.AGE_CRM_CNT);
		
		Dataset<Row> dataCut2018 = dataFrameReader.option(Constants.HEAD,Constants.T)
						.csv(Constants.PATH3 + "2018" + "/" + Constants.AGE_CRM_CNT);
		
		Dataset<Row> dataCut2019 = dataFrameReader.option(Constants.HEAD,Constants.T)
						.csv(Constants.PATH3 + "2019" + "/" + Constants.AGE_CRM_CNT);
		
		Dataset<Row> unionCrimeData = dataCut2015.union(dataCut2016).union(dataCut2017).union(dataCut2018).union(dataCut2019);
		
		
		unionCrimeData.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
							.save(Constants.PATH3 + "Historic/MONTHLY_" + Constants.AGE_CRM_CNT);
		
		
		
		Dataset<Row> dataCut7 = dataFrameReader.option(Constants.HEAD,Constants.T)
											   .csv(Constants.PATH3 + Constants.PATH6 + "/MONTHLY_" + Constants.AGE_CRM_CNT);
		
		
		Dataset<Row> dataCutCrimeSum = dataCut7.groupBy(dataCut7.col(Constants.ARR_MON),
														dataCut7.col(Constants.ARR_OFF),
														dataCut7.col(Constants.ARR_AGE_GRP)
												).agg(functions.sum(dataCut7.col(Constants.AGE_CRM_CNT)).as(Constants.AGE_CRM_SUM))
												 .withColumn(Constants.ARR_YEAR, functions.lit("2019"));
		
		Dataset<Row> dataCutCrimeAvg = dataCutCrimeSum.select(functions.concat(dataCutCrimeSum.col(Constants.ARR_MON),
															  functions.lit("-"),
															  dataCutCrimeSum.col(Constants.ARR_YEAR)).as(Constants.ARR_MON + "-" + Constants.ARR_YEAR),
															  dataCutCrimeSum.col(Constants.ARR_OFF),
															  dataCutCrimeSum.col(Constants.ARR_AGE_GRP),
															  dataCutCrimeSum.col(Constants.AGE_CRM_SUM),
															  dataCutCrimeSum.col(Constants.AGE_CRM_SUM).$div(5).as(Constants.AGE_CRM_AVG)
															);
		
		dataCutCrimeAvg.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
							   .save(Constants.PATH3 + Constants.PATH6 + "/" + Constants.AGE_CRM_CNT);
		
	}





	public void processGenderWiseCrime(DataFrameReader dataFrameReader) {
		
		this.monthlyGenderWiseMovingCrimeAverage(dataFrameReader);
		
	}





	private void monthlyGenderWiseMovingCrimeAverage(DataFrameReader dataFrameReader) {
		Dataset<Row> dataCut2015 = dataFrameReader.option(Constants.HEAD,Constants.T)
				.csv(Constants.PATH3 + "2015" + "/" + Constants.GEN_CRM_CNT);

		Dataset<Row> dataCut2016 = dataFrameReader.option(Constants.HEAD,Constants.T)
						.csv(Constants.PATH3 + "2016" + "/" + Constants.GEN_CRM_CNT);
		
		Dataset<Row> dataCut2017 = dataFrameReader.option(Constants.HEAD,Constants.T)
						.csv(Constants.PATH3 + "2017" + "/" + Constants.GEN_CRM_CNT);
		
		Dataset<Row> dataCut2018 = dataFrameReader.option(Constants.HEAD,Constants.T)
						.csv(Constants.PATH3 + "2018" + "/" + Constants.GEN_CRM_CNT);
		
		Dataset<Row> dataCut2019 = dataFrameReader.option(Constants.HEAD,Constants.T)
						.csv(Constants.PATH3 + "2019" + "/" + Constants.GEN_CRM_CNT);
		
		Dataset<Row> unionCrimeData = dataCut2015.union(dataCut2016).union(dataCut2017).union(dataCut2018).union(dataCut2019);
		
		
		unionCrimeData.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
							.save(Constants.PATH3 + "Historic/MONTHLY_" + Constants.GEN_CRM_CNT);
		
		
		
		Dataset<Row> dataCut7 = dataFrameReader.option(Constants.HEAD,Constants.T)
											   .csv(Constants.PATH3 + Constants.PATH6 + "/MONTHLY_" + Constants.GEN_CRM_CNT);
		
		
		Dataset<Row> dataCutCrimeSum = dataCut7.groupBy(dataCut7.col(Constants.ARR_MON),
														dataCut7.col(Constants.ARR_OFF),
														dataCut7.col(Constants.ARR_GEN)
												).agg(functions.sum(dataCut7.col(Constants.GEN_CRM_CNT)).as(Constants.GEN_CRM_SUM))
												 .withColumn(Constants.ARR_YEAR, functions.lit("2019"));
		
		Dataset<Row> dataCutCrimeAvg = dataCutCrimeSum.select(functions.concat(dataCutCrimeSum.col(Constants.ARR_MON),
															  functions.lit("-"),
															  dataCutCrimeSum.col(Constants.ARR_YEAR)).as(Constants.ARR_MON + "-" + Constants.ARR_YEAR),
															  dataCutCrimeSum.col(Constants.ARR_OFF),
															  dataCut7.col(Constants.ARR_GEN),
															  dataCutCrimeSum.col(Constants.GEN_CRM_SUM),
															  dataCutCrimeSum.col(Constants.GEN_CRM_SUM).$div(5).as(Constants.GEN_CRM_AVG)
															);
		
		dataCutCrimeAvg.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
							   .save(Constants.PATH3 + Constants.PATH6 + "/" + Constants.GEN_CRM_CNT);
		
	}

}
