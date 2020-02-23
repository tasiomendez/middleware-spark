package it.polimi.middleware.spark;

import org.apache.spark.sql.SparkSession;

public class CarAccidents {
	
	private static final String APP_NAME = "NYPD Motor Vehicle Collisions";

	public static void main(String[] args) {

		final String master = args.length > 0 ? args[0] : "local[1]";
		final String file = args.length > 1 ? args[1] : "./files/NYPD_Motor_Vehicle_Collisions.csv";

		final SparkSession spark = SparkSession //
				.builder() //
				.master(master) //
				.appName(APP_NAME) //
				.getOrCreate();

		spark.close();

	}

}
