package it.polimi.middleware.spark.dataset;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Structure of the dataset. It includes the column name as well as metadata such as the 
 * data type on each column and if it could include null values. Provides methods 
 * to get groups of columns which are used to perform data analysis.
 */

public enum Columns {
	
	DATE("DATE", DataTypes.DateType, false), 
	TIME("TIME", DataTypes.StringType, false), 
	BOROUGH("BOROUGH", DataTypes.StringType, true), 
	ZIP_CODE("ZIP_CODE", DataTypes.IntegerType, true), 
	LATITUDE("LATITUDE", DataTypes.DoubleType, true), 
	LONGITUDE("LONGITUDE", DataTypes.DoubleType, true),
    LOCATION("LOCATION", DataTypes.StringType, true), 
    ON_STREET_NAME("ON_STREET_NAME", DataTypes.StringType, true), 
    CROSS_STREET_NAME("CROSS_STREET_NAME", DataTypes.StringType, true), 
    OFF_STREET_NAME("OFF_STREET_NAME", DataTypes.StringType, true),
    NUMBER_OF_PERSONS_INJURED("NUMBER_OF_PERSONS_INJURED", DataTypes.IntegerType, false), 
    NUMBER_OF_PERSONS_KILLED("NUMBER_OF_PERSONS_KILLED", DataTypes.IntegerType, false),
    NUMBER_OF_PEDESTRIANS_INJURED("NUMBER_OF_PEDESTRIANS_INJURED", DataTypes.IntegerType, false), 
    NUMBER_OF_PEDESTRIANS_KILLED("NUMBER_OF_PEDESTRIANS_KILLED", DataTypes.IntegerType, false),
    NUMBER_OF_CYCLIST_INJURED("NUMBER_OF_CYCLIST_INJURED", DataTypes.IntegerType, false), 
    NUMBER_OF_CYCLIST_KILLED("NUMBER_OF_CUCLIST_KILLED", DataTypes.IntegerType, false),
    NUMBER_OF_MOTORIST_INJURED("NUMBER_OF_MOTORIST_INJURED", DataTypes.IntegerType, false), 
    NUMBER_OF_MOTORIST_KILLED("NUMBER_OF_MOTORIST_KILLED", DataTypes.IntegerType, false),
    CONTRIBUTING_FACTOR_VEHICLE_1("CONTRIBUTING_FACTOR_VEHICLE_1", DataTypes.StringType, true), 
    CONTRIBUTING_FACTOR_VEHICLE_2("CONTRIBUTING_FACTOR_VEHICLE_2", DataTypes.StringType, true),
    CONTRIBUTING_FACTOR_VEHICLE_3("CONTRIBUTING_FACTOR_VEHICLE_3", DataTypes.StringType, true), 
    CONTRIBUTING_FACTOR_VEHICLE_4("CONTRIBUTING_FACTOR_VEHICLE_4", DataTypes.StringType, true),
    CONTRIBUTING_FACTOR_VEHICLE_5("CONTRIBUTING_FACTOR_VEHICLE_5", DataTypes.StringType, true), 
    UNIQUE_KEY("UNIQUE_KEY", DataTypes.IntegerType, false), 
    VEHICLE_TYPE_CODE_1("VEHICLE_TYPE_CODE_1", DataTypes.StringType, true),
    VEHICLE_TYPE_CODE_2("VEHICLE_TYPE_CODE_2", DataTypes.StringType, true), 
    VEHICLE_TYPE_CODE_3("VEHICLE_TYPE_CODE_3", DataTypes.StringType, true), 
    VEHICLE_TYPE_CODE_4("VEHICLE_TYPE_CODE_4", DataTypes.StringType, true),
    VEHICLE_TYPE_CODE_5("VEHICLE_TYPE_CODE_5", DataTypes.StringType, true);
    
    private String name;
    private DataType type;
    private Boolean isNullable;
    
    Columns(String name, DataType type, Boolean isNullable) {
    	this.name = name;
    	this.type = type;
    	this.isNullable = isNullable;
    }
    
    public String getName() {
    	return this.name;
    }
    
    public DataType getType() {
    	return this.type;
    }
    
    public Boolean isNullable() {
    	return this.isNullable;
    }
    
    public static Set<Columns> getFeatures() {
    	Set<Columns> set = new HashSet<Columns>();
    	set.add(NUMBER_OF_PERSONS_INJURED);
    	set.add(NUMBER_OF_PERSONS_KILLED);
    	set.add(NUMBER_OF_PEDESTRIANS_INJURED);
    	set.add(NUMBER_OF_PEDESTRIANS_KILLED);
    	set.add(NUMBER_OF_CYCLIST_INJURED);
    	set.add(NUMBER_OF_CYCLIST_KILLED);
    	set.add(NUMBER_OF_MOTORIST_INJURED);
    	set.add(NUMBER_OF_MOTORIST_KILLED);
    	return set;
    }
    
    public static Set<Columns> getDeaths() {
    	Set<Columns> set = new HashSet<Columns>();
    	set.add(NUMBER_OF_PERSONS_KILLED);
    	set.add(NUMBER_OF_PEDESTRIANS_KILLED);
    	set.add(NUMBER_OF_CYCLIST_KILLED);
    	set.add(NUMBER_OF_MOTORIST_KILLED);
    	return set;
    }
    
    public static Set<Columns> getContributingFactors() {
    	Set<Columns> set = new HashSet<Columns>();
    	set.add(CONTRIBUTING_FACTOR_VEHICLE_1);
    	set.add(CONTRIBUTING_FACTOR_VEHICLE_2);
    	set.add(CONTRIBUTING_FACTOR_VEHICLE_3);
    	set.add(CONTRIBUTING_FACTOR_VEHICLE_4);
    	set.add(CONTRIBUTING_FACTOR_VEHICLE_5);
    	return set;
    }
    
    public static Set<Columns> getVehicleTypes() {
    	Set<Columns> set = new HashSet<Columns>();
    	set.add(VEHICLE_TYPE_CODE_1);
    	set.add(VEHICLE_TYPE_CODE_2);
    	set.add(VEHICLE_TYPE_CODE_3);
    	set.add(VEHICLE_TYPE_CODE_4);
    	set.add(VEHICLE_TYPE_CODE_5);
    	return set;
    }
    
    public static String[] getNames(ColumnsSet func) {
    	return Arrays.stream(func.get().toArray())
    			.map(item -> ((Columns) item).getName()).toArray(String[]::new);
    }
    
    @FunctionalInterface
    public interface ColumnsSet {
        Set<Columns> get();
    }

}
