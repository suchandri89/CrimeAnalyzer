package com.ljmu.pancrime;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;


public class AnnualDataRefiner implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public void cleanseData(SparkSession session, DataFrameReader dataFrameReader) {
		
		//fetching New York city borough distribution data
		Dataset<Row> boroughData = dataFrameReader.option(Constants.HEAD,Constants.T).csv(Constants.PATH4);
		
		//iterating for 7 years
		for(Map.Entry<String, String> entry:Constants.YEAR_MAP.entrySet()) {
			
			//method to fetch legacy data
			Dataset<Row> dataCut1 = initialCleanser(entry.getKey(), dataFrameReader);
			
			//method to fetch only required fields from entire legacy dataset
			Dataset<Row> dataCut2 = fetchFields(entry.getKey(), dataCut1);	    
		    
		    //setting borough string column and month column in the new dataset
			addColumns(entry.getKey(), 
					   session, 
					   dataFrameReader, 
					   dataCut2, 
					   boroughData
					   );
		         
		}
	}

	private void addColumns(String year, SparkSession session, DataFrameReader dataFrameReader, Dataset<Row> dataCut2,
			Dataset<Row> boroughData) {
		
		//setting borough string in datacut3    
	    Dataset<Row> dataCut3 = dataCut2.join(boroughData,dataCut2.col(Constants.ARR_BOR).equalTo(boroughData.col(Constants.ARR_BOR)), Constants.INNER_JOIN);
	    
	    dataCut3 = dataCut3.select(dataCut3.col(Constants.ARR_ID),
	    		   dataCut3.col(Constants.ARR_DATE), 
	    		   dataCut3.col(Constants.ARR_OFF), 
	    		   dataCut3.col(Constants.ARR_BOR_STR), 
	    		   dataCut3.col(Constants.ARR_AGE_GRP), 
	    		   dataCut3.col(Constants.ARR_GEN)
		    	   );
	    //saving data to intermediate csv file for further analysis
	    dataCut3.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
	    				.save(Constants.PATH3 + year + Constants.PATH2 + year + Constants.V3);
	    
	   
	    // Define and register a one-argument UDF
	    @SuppressWarnings({ "serial", "rawtypes" })
		UDF1 getMonth = new UDF1<String, String>() {
	        @Override
	        public String call(String x) {
	      	  String month = "";
	      	  if(x!=null) {
	      		String dateParts[] = x.split("-");
	      		try {
	    			month = dateParts[1];
	    		}catch(Exception e) {
	    			String dateParts1[] = x.split("/");
	    			month = dateParts1[0];
	    		}
	      		if(month.length()>0) {
	      			return Constants.MONTH_MAP.get(month);
	      		}
	      	  }
	          return month;
	        }
	      };
	    
	    session.udf().register(Constants.GET_MONTH, getMonth, DataTypes.StringType);
	    
	    //find month column from date column
	    Dataset<Row> dataCut4 = dataCut3.selectExpr(Constants.ARR_ID,
	    											Constants.ARR_DATE,
										    		Constants.ARR_OFF,
										    		Constants.ARR_BOR_STR,
										    		Constants.ARR_AGE_GRP,
										    		Constants.ARR_GEN,
										    		Constants.CALL_GET_MONTH);
	    //saving data to intermediate csv file for further analysis
	    dataCut4.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
	    				.save(Constants.PATH3 + year + Constants.PATH2 + year + Constants.V4);
		
	}

	private Dataset<Row> fetchFields(String year, Dataset<Row> dataCut1) {
		//records containing major crime types of this report
	    Dataset<Row> dataCut2 = dataCut1.where(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_A1)
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_A2))
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_B1))
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_C1))
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_D1))
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_F1))//online
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_F2))
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_L1))
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_H1))
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_H2))
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_K1))
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_M1))//molestation
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_M2))
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_P1))
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_R1))
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_R2))
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_T1))//traffic death
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_T2))//theft
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_T3))//online
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_U1))
	    						.or(dataCut1.col(Constants.ARR_OFF).contains(Constants.TYPE_W1))
	    						);
	    
	    //saving data to intermediate csv file for further analysis
	    dataCut2.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
	    				.save(Constants.PATH3 + year + Constants.PATH2 + year + Constants.V2);
	    
	    return dataCut2;
	}

	private Dataset<Row> initialCleanser(String year, DataFrameReader dataFrameReader) {
		//.csv method is used to read data from a csv file.     
	    Dataset<Row> originalData = dataFrameReader.option(Constants.HEAD,Constants.T)
	    							.csv(Constants.PATH1 + Constants.PATH2 + year + Constants.FETCH_ORIGINAL); 
			    
	    //fetching required columns from the original dataset
	    Dataset<Row> dataCut1 = originalData.select(originalData.col(Constants.ARR_ID),
	    											originalData.col(Constants.ARR_DATE), 
										    		originalData.col(Constants.ARR_OFF), 
										    		originalData.col(Constants.ARR_BOR), 
										    		originalData.col(Constants.ARR_AGE_GRP), 
										    		originalData.col(Constants.ARR_GEN)
	    							);
	    
	    //dropping records having nulls values
	    dataCut1.na().drop();
	    		    
	    //saving data to intermediate csv file for further analysis
	    dataCut1.write().option(Constants.HEAD,Constants.T).format(Constants.OUT_FORMAT)
	    				.save(Constants.PATH3 + year + Constants.PATH2 + year + Constants.V1);
	    
	    return dataCut1;
	}

}
