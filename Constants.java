package com.ljmu.pancrime;

import java.util.HashMap;
import java.util.Map;

public interface Constants {

	Map<String, String> YEAR_MAP =  new HashMap<String,String>() {{
		put("2015","2015");
		put("2016","2016");
		put("2017","2017");
		put("2018","2018");
		put("2019","2019");
		put("2020","2020");
		put("2021","2021");
	}};
	Map<String, String> MONTH_MAP =  new HashMap<String,String>() {{
		put("01","Jan");
		put("02","Feb");
		put("03","Mar");
		put("04","Apr");
		put("05","May");
		put("06","Jun");
		put("07","Jul");
		put("08","Aug");
		put("09","Sep");
		put("10","Oct");
		put("11","Nov");
		put("12","Dec");
	}};
	final String HEAD = "header";
	final String T = "true";
	final String PATH1 = "src/main/resources/data/";
	final String PATH2 = "/NYPD_Arrest_Data_Year_to_Date_";
	final String PATH3 = "src/main/resources/datacut/";
	final String PATH4 = "src/main/resources/data/Borogh_File.csv";
	final String PATH5 = "/MONTHLY_CRIME_COUNT";
	final String PATH6 = "Historic";
	final String PATH7 = "/MONTHLY_CRIME_SUM";
	final String FETCH_ORIGINAL = "_V0.csv";
	final String ARR_ID = "ARREST_KEY";
	final String ARR_DATE = "ARREST_DATE";
	final String ARR_OFF = "OFNS_DESC";
	final String ARR_BOR = "ARREST_BORO";
	final String ARR_BOR_STR = "ARREST_BORO_STRING";
	final String ARR_AGE_GRP = "AGE_GROUP";
	final String ARR_GEN = "PERP_SEX";
	final String OUT_FORMAT = "csv";
	final String V1 = "_V1";
	final String V2 = "_V2";
	final String V3 = "_V3";
	final String V4 = "_V4";
	final String TYPE_A1 = "ARSON";//1
	final String TYPE_A2 = "ASSAULT";//2
	final String TYPE_B1 = "BURGLARY";//3
	final String TYPE_C1 = "CHILD ABANDONMENT";//4
	final String TYPE_D1 = "DRUGS";//5
	final String TYPE_F1 = "FORGERY";//online 6
	final String TYPE_F2 = "FRAUDS";//7
	final String TYPE_L1 = "LARCENY";//8
	final String TYPE_H1 = "HARRASSMENT";//9
	final String TYPE_H2 = "HOMICIDE";//10
	final String TYPE_K1 = "KIDNAPPING";//11
	final String TYPE_M1 = "SEX CRIMES";//molestation 12
	final String TYPE_M2 = "MURDER";//13
	final String TYPE_P1 = "POSSESSION OF STOLEN PROPERTY";//14
	final String TYPE_R1 = "RAPE";//15
	final String TYPE_R2 = "ROBBERY";//16
	final String TYPE_T1 = "HOMICIDE-NEGLIGENT-VEHICLE";//traffic death 17
	final String TYPE_T2 = "THEFT-FRAUD";//theft 18
	final String TYPE_T3 = "THEFT OF SERVICES";//online 19
	final String TYPE_U1 = "UNAUTHORIZED USE OF A VEHICLE";//20
	final String TYPE_W1 = "WEAPONS";//21
	final String INNER_JOIN = "inner";
	final String DATE_FORMAT_1 = "MM/dd/yyyy";
	final String DATE_FORMAT_2 = "dd-MM-yyyy";
	final String MON = "MMM";
	final String GET_MONTH = "getMonth";
	final String CALL_GET_MONTH = "getMonth(ARREST_DATE) as Month";
	final String ARR_MON = "Month";
	final String ARR_YEAR = "Year";
	final String ARR_ONLINE_CNT = "ONLINE_COUNT";
	final String ARR_OFFLINE_CNT = "OFFLINE_COUNT";
	final String ARR_CRM_CNT = "CRIME_COUNT";
	final String CRM_SUM = "CRIME_SUM";
	final String CRM_AVG = "CRIME_AVG";
	final String BOR_CRM_CNT = "BOROUGH_CRIME_COUNT";
	final String BOR_CRM_SUM = "BOROUGH_CRIME_SUM";
	final String BOR_CRM_AVG = "BOROUGH_CRIME_AVG";
	final String AGE_CRM_CNT = "AGE_CRIME_COUNT";
	final String AGE_CRM_SUM = "AGE_CRIME_SUM";
	final String AGE_CRM_AVG = "AGE_CRIME_AVG";
	final String GEN_CRM_CNT = "GENDER_CRIME_COUNT";
	final String GEN_CRM_SUM = "GENDER_CRIME_SUM";
	final String GEN_CRM_AVG = "GENDER_CRIME_AVG";
	
}
