package it.polimi.middleware.spark.dataset;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.date_sub;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.mean;
import static org.apache.spark.sql.functions.next_day;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConversions;
import scala.collection.Seq;

public class Dataframe {

	public static final String DATE_FORMAT = "MM/dd/yyyy";
	public static final String FIRST_DAY_WEEK = "sunday";
	public static final String CONTRIBUTING_FACTOR = "CONTRIBUTING_FACTOR";
	
	private static Logger logger = Logger.getLogger(Dataframe.class.getName());
	
	private Dataset<Row> raw;

	public Dataframe (SparkSession spark, String file) {

		final List<StructField> fields = new ArrayList<>();
		for (Columns col: Columns.values()) 
			fields.add(DataTypes.createStructField(col.getName(), col.getType(), col.isNullable()));

		final StructType schema = DataTypes.createStructType(fields);

		logger.info("Reading dataset from " + file);
		double start = System.currentTimeMillis();

		this.raw = spark.read()
				.option("header", "true")
				.option("delimiter", ",")
				.option("dateFormat", DATE_FORMAT)
				.schema(schema).csv(file);
		
		double time = (System.currentTimeMillis() - start) / 1000;
		logger.info("File read in " + time + " seconds");

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
	 * @return dataframe
	 */

	public Dataset<Row> q2() {
		
		// Count number of accidents for each contributing factor
		final List<Dataset<Row>> datasets = this.groupContributingFactors(this.clean(), false);
		final Dataset<Row> df1 = joinContributingFactorColumns(datasets, "N_ACCIDENTS");
		
		// Count number of lethal accidents for each contributing factor
		final List<Dataset<Row>> datasets2 = this.groupContributingFactors(this.clean(), true);
		datasets2.add(df1);
		final Dataset<Row> df2 = joinContributingFactorColumns(datasets2, "N_LETHAL_ACCIDENTS");
		
		return df2.withColumn("PERCENTAGE_LETHAL_ACCIDENTS", expr("N_LETHAL_ACCIDENTS / N_ACCIDENTS * 100"))
				.orderBy(CONTRIBUTING_FACTOR);
	}
	
	/**
	 * Provides a list of dataframes since there are five different columns
	 * which include information about contributing factors.
	 * 
	 * @param dataframe
	 * @param filtered true for filtering, otherwise false
	 * @return list of dataframes grouped for each column of contributing factor
	 */
	
	private List<Dataset<Row>> groupContributingFactors(Dataset<Row> dataframe, Boolean filtered) {
		final Dataset<Row> df = (filtered) ? dataframe.filter(Dataframe::killed) : dataframe;
		final List<Dataset<Row>> lst = new ArrayList<Dataset<Row>>();
		for (Columns col: Columns.getContributingFactors())
			lst.add(this.groupContributingFactor(df, col.getName()));
		return lst;
	}
	
	/**
	 * Group by contributing factor on one column.
	 * 
	 * @param dataframe
	 * @param on name of contributing factor column
	 * @return dataframe grouped
	 */
	
	private Dataset<Row> groupContributingFactor(Dataset<Row> dataframe, String on) {
		return dataframe.withColumnRenamed(on, CONTRIBUTING_FACTOR)
				.groupBy(CONTRIBUTING_FACTOR).agg(count(CONTRIBUTING_FACTOR).alias(on));
	}
	
	/**
	 * Sum the columns of contributing factor
	 * 
	 * @param list list of dataframes
	 * @param name name of the new column
	 * @return dataframe
	 */
	
	private Dataset<Row> joinContributingFactorColumns(List<Dataset<Row>> list, String name) {
		// Name of the columns of Contributing Factors one to five
		Seq<String> columns = JavaConversions.asScalaBuffer(Arrays.asList(Columns.getNames(Columns::getContributingFactors)));
		
		final Dataset<Row> dfs = this.join(list, CONTRIBUTING_FACTOR);
		return dfs.filter(dfs.col(CONTRIBUTING_FACTOR).isNotNull())
				.na().fill(0, Columns.getNames(Columns::getContributingFactors))
				.withColumn(name, expr(columns.mkString("+"))).drop(columns);
	}
	
	/**
	 * Perform a join operation over a list of dataframes.
	 * 
	 * @param dataframes
	 * @param on column where perform join
	 * @return datasets joined
	 */
	
	private Dataset<Row> join(List<Dataset<Row>> dataframes, String on) {
		if (dataframes.size() == 1) return dataframes.remove(0);
		final List<String> join = new ArrayList<String>(Arrays.asList(on));
		return dataframes.remove(0).join(this.join(dataframes, on), JavaConversions.asScalaBuffer(join), "full_outer");
	}
	
	/**
	 * Q3. Number of accidents and average of lethal accidents per week per borough.
	 * 
	 * @return dataframe
	 */

	public Dataset<Row> q3() {
		
		final List<String> join = new ArrayList<String>();
		join.add(Columns.BOROUGH.getName());
		join.add("WEEK");
		
		final Dataset<Row> df1 = this.groupBoroughsByWeek(this.clean(), "N_ACCIDENTS", false);
		final Dataset<Row> df2 = this.groupBoroughsByWeek(this.clean(), "N_LETHAL_ACCIDENTS", true);
		
		return df1.join(df2, JavaConversions.asScalaBuffer(join), "full_outer")
				.na().fill(0).orderBy(Columns.BOROUGH.getName(), "WEEK");
	}
	
	public Dataset<Row> q3mean() {
		return q3().groupBy(Columns.BOROUGH.getName()).agg(
				mean("N_LETHAL_ACCIDENTS").alias("MEAN_LETHAL_ACCIDENTS"));
	}
	
	/**
	 * Group dataframe by boroughs and week.
	 * 
	 * @param dataframe
	 * @param alias new column name
	 * @param filtered true for filtering, otherwise false 
	 * @return dataframe
	 */
	
	private Dataset<Row> groupBoroughsByWeek(Dataset<Row> dataframe, String alias, Boolean filtered) {
		final Dataset<Row> df = (filtered) ? dataframe.filter(Dataframe::killed) : dataframe;
		return df.filter(df.col(Columns.BOROUGH.getName()).isNotNull())
				.withColumn("WEEK", next_day(date_sub(df.col(Columns.DATE.getName()), 1), FIRST_DAY_WEEK))
				.groupBy(Columns.BOROUGH.getName(), "WEEK").agg(
						count("WEEK").alias(alias))
				.orderBy(Columns.BOROUGH.getName(), "WEEK");
	}
	
	@SuppressWarnings("unchecked")
	public Dataset<Row> run(String func) {
		logger.info("Starting data processing...");
		double start = System.currentTimeMillis();
		Dataset<Row> df = null;
		try {
			df = (Dataset<Row>) Dataframe.class.getMethod(func).invoke(this);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		double time = (System.currentTimeMillis() - start) / 1000;
		logger.info("Data processed in " + time + " seconds");
		return df;
	}

}
