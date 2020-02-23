package it.polimi.middleware.spark.dataset;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.date_sub;
import static org.apache.spark.sql.functions.next_day;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Dataframe {

	public static final String DATE_FORMAT = "MM/dd/yyyy";
	public static final String FIRST_DAY_WEEK = "sunday";
	
	private Dataset<Row> raw;

	public Dataframe (SparkSession spark, String file) {

		final List<StructField> fields = new ArrayList<>();
		for (Columns col: Columns.values()) 
			fields.add(DataTypes.createStructField(col.getName(), col.getType(), col.isNullable()));

		final StructType schema = DataTypes.createStructType(fields);

		this.raw = spark.read()
				.option("header", "true")
				.option("delimiter", ",")
				.option("dateFormat", DATE_FORMAT)
				.schema(schema).csv(file);

	}
	
	public Dataset<Row> getRaw() {
		return this.raw;
	}
	
	/**
	 * Clean dataframe for data analysis removing rows with null in 
	 * features values.
	 * 
	 * @return cleaned dataframe
	 */
	
	private Dataset<Row> clean() {
		return this.raw.na().drop(Columns.getNames(Columns::getFeatures));
	}
	
	/**
	 * Filter function to get rows which includes at least one dead.
	 * 
	 * @param row
	 * @return true if row is valid, otherwise false
	 */

	private static boolean killed(Row row) {
		for (Columns col: Columns.getDeaths())
			if (row.<Integer>getAs(col.getName()) > 0)
				return true;
		return false;
	}
	
	/**
	 * Q1. Number of lethal accidents per week.
	 * 
	 * @param dataframe
	 * @return dataframe
	 */

	public Dataset<Row> q1() {
		final Dataset<Row> filtered = this.clean().filter(Dataframe::killed);

		// Get the next sunday from the date for grouping by week
		return filtered.withColumn("WEEK", next_day(date_sub(filtered.col(Columns.DATE.getName()), 1), FIRST_DAY_WEEK))
				.groupBy("WEEK").agg(
						count("WEEK").alias("N_ACCIDENTS"))
				.orderBy("WEEK");
	}
	
	/**
	 * Q2. Number of accidents per contributing factor.
	 * 
	 * @param dataframe
	 * @return dataframe
	 */

	public Dataset<Row> q2() {
		return null;
	}
	
	/**
	 * Q3. Number of accidents and average of lethal accidents per week per borough.
	 * 
	 * @param dataframe
	 * @return dataframe
	 */

	public Dataset<Row> q3() {
		return null;
	}

}
