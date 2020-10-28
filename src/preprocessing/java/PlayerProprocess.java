import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import org.apache.spark.sql.functions;


import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlayerProprocess {

	public static String INPUT_FILE = "src/main/resources/demographics.json";

	public static String translate(Map<String, String> map, Column c){
System.out.println(map.get("Super League 1"));
		System.out.println(c);
		return map.get(c.toString());
	}

	public static String playerPreprocess(String db_user, String db_pass) {
		String out = "";

		SparkSession session = SparkSession.builder().master("local").appName("jsonreader").getOrCreate();
		Dataset<Row> soccer_players = session.read().json(INPUT_FILE);
		
		// create a static mapping from League to Countries
		Map<String, String> map = new HashMap<String, String>();
		// Greece
		map.put("Super League 1", "Greece");
		map.put("Super League 2", "Greece");
		map.put("Football League", "Greece");
		// England
		map.put("Premier League", "England");
		map.put("Football League Championship", "England");
		map.put("Football League One", "England");
		// France
		map.put("Ligue 1", "France");
		map.put("Ligue 2", "France");
		map.put("National", "France");
		map.put("National 2 A", "France");
		// Spain
		map.put("La Liga", "Spain");
		map.put("La Liga 2", "Spain");
		map.put("Segunda B I", "Spain");
		// Italy
		map.put("Serie A", "Italy");
		map.put("Serie B", "Italy");
		map.put("Serie C Girone A", "Italy");
		map.put("Serie C Girone B", "Italy");
		// Portugal
		map.put("Primeira Liga", "Portugal");
		map.put("LigaPro", "Portugal");
		map.put("Campeonato de Portugal Serie A", "Portugal");

		// remove the first line
		soccer_players = soccer_players.filter("Full_Name is not null");

		// cast Age to Integer
		soccer_players = soccer_players.withColumn("Age", soccer_players.col("Age").cast("Integer"));

		// replace empty string with nulls for all the columns
		for(String column: soccer_players.columns())
			soccer_players = soccer_players.withColumn(column, functions.when(soccer_players.col(column).equalTo(""), null).otherwise(soccer_players.col(column)));
		soccer_players = soccer_players.withColumn("Squad_Number", functions.when(soccer_players.col("Squad_Number").equalTo("Not set"), null).otherwise(soccer_players.col("Squad_Number")));


		// get the name of the country of the league from the file's name
		// create a new column with this constant value
		//String league_country = INPUT_FILE.split("/")[INPUT_FILE.split("/").length-1].split("_")[1].toUpperCase();
		//soccer_players = soccer_players.withColumn("County_league", functions.lit(league_country));
		//soccer_players = soccer_players.withColumn("County_league", functions.col(translate(map, soccer_players.col("League"))));
		//soccer_players = soccer_players.withColumn("County_league", functions.when(soccer_players.col("League").isNotNull(), soccer_players.select("League")));

		//write the result to CSV
		soccer_players.write()
				.format("com.databricks.spark.csv")
				.option("header", "true")
				.save("src/main/resources/out.csv");

		// write the result dataset to Postgres
//		soccer_players.write()
//				.format("jdbc")
//				.mode("append")
//				.option("url", "jdbc:postgresql://localhost:5432/Freekick")
//				.option("dbtable", "info.player")
//				.option("user", db_user)
//				.option("password", db_pass)
//				.save();

		return out;
	}

	// Function to flatten JSON file
	// Not used anywhere in this file
	private static Dataset flattenJSONdf(Dataset<Row> ds) {

		StructField[] fields = ds.schema().fields();

		List<String> fieldsNames = new ArrayList<>();
		for (StructField s : fields) {
			fieldsNames.add(s.name());
		}

		for (int i = 0; i < fields.length; i++) {

			StructField field = fields[i];
			DataType fieldType = field.dataType();
			String fieldName = field.name();

			if (fieldType instanceof ArrayType) {
				List<String> fieldNamesExcludingArray = new ArrayList<String>();
				for (String fieldName_index : fieldsNames) {
					if (!fieldName.equals(fieldName_index))
						fieldNamesExcludingArray.add(fieldName_index);
				}
				System.out.print("here "+fieldNamesExcludingArray);

				List<String> fieldNamesAndExplode = new ArrayList<>(fieldNamesExcludingArray);
				//String s = String.format("explode_outer(%s) as %s", fieldName, fieldName);
				String s = String.format("struct(%s) as %s", fieldName, fieldName+"_new");

				fieldNamesAndExplode.add(s);

				String[]  exFieldsWithArray = new String[fieldNamesAndExplode.size()];
				Dataset exploded_ds = ds.selectExpr(fieldNamesAndExplode.toArray(exFieldsWithArray));

				// explodedDf.show();

				return flattenJSONdf(exploded_ds);

			}
			else if (fieldType instanceof StructType) {

				String[] childFieldnames_struct = ((StructType) fieldType).fieldNames();

				List<String> childFieldnames = new ArrayList<>();
				for (String childName : childFieldnames_struct) {
					childFieldnames.add(fieldName + "." + childName);
				}

				List<String> newfieldNames = new ArrayList<>();
				for (String fieldName_index : fieldsNames) {
					if (!fieldName.equals(fieldName_index))
						newfieldNames.add(fieldName_index);
				}

				newfieldNames.addAll(childFieldnames);

				List<Column> renamedStrutctCols = new ArrayList<>();

				for(String newFieldNames_index : newfieldNames){
					renamedStrutctCols.add( new Column(newFieldNames_index.toString()).as(newFieldNames_index.toString().replace(".", "_")));
				}

				Seq renamedStructCols_seq = JavaConverters.collectionAsScalaIterableConverter(renamedStrutctCols).asScala().toSeq();

				Dataset ds_struct = ds.select(renamedStructCols_seq);

				return flattenJSONdf(ds_struct);
			}
			else{

			}

		}
		return ds;
	}
}

