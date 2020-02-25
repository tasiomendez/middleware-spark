package it.polimi.middleware.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import it.polimi.middleware.spark.dataset.Dataframe;

public class CarAccidents {
	
	private static final String APP_NAME = "NYPD Motor Vehicle Collisions";

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("Dataframe").setLevel(Level.OFF);

		final String master = args.length > 0 ? args[0] : "local[1]";
		final String file = args.length > 1 ? args[1] : "./files/NYPD_Motor_Vehicle_Collisions.csv";

		final SparkSession spark = SparkSession.builder().master(master)
				.appName(APP_NAME).getOrCreate();
		
		final Dataframe df = new Dataframe(spark, file);
		
		// df.q1().show();
		// df.q2().show();
//		df.q3().show();
		
		df.run("q3mean").show();
		
		spark.close();

	}

}
