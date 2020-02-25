package it.polimi.middleware.spark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import it.polimi.middleware.spark.dataset.Dataframe;

public class CarAccidents {
	
	private static final String APP_NAME = "NYPD Motor Vehicle Collisions";
	
	private static final Logger logger = Logger.getLogger(CarAccidents.class.getName());

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);

//		final String master = args.length > 0 ? args[0] : "local[1]";
//		final String file = args.length > 1 ? args[1] : "./files/NYPD_Motor_Vehicle_Collisions.csv";
		
		final Options options = new Options();
		final CommandLineParser parser = new DefaultParser();
		final HelpFormatter formatter = new HelpFormatter();
		
		final Option _master = new Option("m", "master", true, "spark master address");
		_master.setType(String.class);
		_master.setArgName("ADDRESS");
		options.addOption(_master);
		
		final Option _file = new Option("f", "file", true, "CSV file");
		_file.setType(String.class);
		_file.setArgName("FILE");
		options.addOption(_file);
		
		final Option _question = new Option("q", "question", true, "data to access");
		_question.setType(String.class);
		_question.setArgName("QUESTION");
		options.addOption(_question);
		
		final Option _show = new Option("s", "show", true, "show X results");
		_show.setType(Number.class);
		_show.setArgName("X");
		_show.setOptionalArg(true);
		options.addOption(_show);
			    
	    try {
	    	
			final CommandLine cmd = parser.parse(options, args);
			final String master = cmd.getOptionValue("spark", "local[1]");
			final String file = cmd.getOptionValue("file", "./files/NYPD_Motor_Vehicle_Collisions.csv");
			final String question = cmd.getOptionValue("question", "q1");			
			final int show = Integer.parseInt(cmd.getOptionValue("show", "-1"));
			
			final SparkSession spark = SparkSession.builder().master(master)
					.appName(APP_NAME).getOrCreate();
			logger.info("Spark session created succesfully");
			
			final Dataframe df = new Dataframe(spark, file);
			
			if (show < 0 && cmd.hasOption("show"))
				df.run(question).show(false);
			else if (show > 0)
				df.run(question).show(show, false);
			else df.run(question);
			
			spark.close();
			logger.info("Spark session closed succesfully");
			
		} catch (ParseException e) {
			System.err.println("[ERROR] " + e.getLocalizedMessage() + "\n");
		    formatter.printHelp(CarAccidents.class.getSimpleName(), options, true);
		    System.exit(1);
		} 

	}

}
