package spark;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import spark.dataset.Dataframe;

public class CarAccidents {
	
	private static final String APP_NAME = "NYPD Motor Vehicle Collisions";
	
	private static final Logger logger = Logger.getLogger(CarAccidents.class.getName());

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		
		final Options options = new Options();
		final CommandLineParser parser = new GnuParser();
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
		_question.setArgs(Option.UNLIMITED_VALUES);
		_question.setValueSeparator(' ');
		_question.setRequired(true);
		options.addOption(_question);
		
		final Option _show = new Option("s", "show", true, "show X results");
		_show.setType(Number.class);
		_show.setArgName("X");
		_show.setOptionalArg(true);
		options.addOption(_show);
		
		final Option _cached = new Option("c", "cached", false, "cached dataframes on spark slaves");
		options.addOption(_cached);
			    
	    try {
	    	
			final CommandLine cmd = parser.parse(options, args);
			final String master = cmd.getOptionValue("master", "local[1]");
			final String file = cmd.getOptionValue("file", "./files/NYPD_Motor_Vehicle_Collisions.csv");
			final String[] questions = cmd.getOptionValues("question");			
			final int show = Integer.parseInt(cmd.getOptionValue("show", "-1"));
			
			final SparkSession spark = SparkSession.builder().master(master)
					.appName(APP_NAME).getOrCreate();
			logger.info("Spark session created succesfully");
			
			final Dataframe df = new Dataframe(spark, file, cmd.hasOption("cached"));
			
			for (int i = 0; i < questions.length; i++)
				if (cmd.hasOption("show"))
					Dataframe.show(df.run(questions[i]), show, false);
				else df.run(questions[i]);
			
			spark.close();
			logger.info("Spark session closed succesfully");
			
		} catch (ParseException e) {
			System.err.println("[ERROR] " + e.getLocalizedMessage() + "\n");
		    formatter.printHelp(CarAccidents.class.getSimpleName(), options, true);
		    System.exit(1);
		} 

	}

}
