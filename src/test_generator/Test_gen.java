import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple5;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Exercise_1 {
	
	public static String basicAnalysis(JavaSparkContext ctx) throws IOException {
		String out = "";

		SparkSession spark = SparkSession
				.builder()
				.appName("Spark Example - Read JSON to RDD")
				.master("local[2]")
				.getOrCreate();

		String jsonPath = "raw_tests.json";
		JavaRDD<Row> items = spark.read().json(jsonPath).toJavaRDD();
		JavaPairRDD<String,Tuple5<Tuple2<String,Float>,Tuple2<String,Float>,Tuple2<String,Float>,Tuple2<String,Float>,Tuple2<String,Float>>> playerStatsRDD = items.mapToPair(t -> {
			java.lang.Float stress = (
					java.lang.Float.parseFloat(t.get(30).toString())+ java.lang.Float.parseFloat(t.get(16).toString())+
					java.lang.Float.parseFloat(t.get(40).toString())+ java.lang.Float.parseFloat(t.get(33).toString())+
					java.lang.Float.parseFloat(t.get(11).toString())+ java.lang.Float.parseFloat(t.get(2).toString())+
					java.lang.Float.parseFloat(t.get(48).toString())+ java.lang.Float.parseFloat(t.get(1).toString())+
					java.lang.Float.parseFloat(t.get(4).toString())+ java.lang.Float.parseFloat(t.get(12).toString())+
					java.lang.Float.parseFloat(t.get(23).toString())+ java.lang.Float.parseFloat(t.get(51).toString())+
					java.lang.Float.parseFloat(t.get(45).toString())+ java.lang.Float.parseFloat(t.get(14).toString())+
					java.lang.Float.parseFloat(t.get(9).toString())+ java.lang.Float.parseFloat(t.get(15).toString())+
					java.lang.Float.parseFloat(t.get(43).toString())+ java.lang.Float.parseFloat(t.get(19).toString())+
					java.lang.Float.parseFloat(t.get(10).toString())+ java.lang.Float.parseFloat(t.get(31).toString()) ) / 20;

			java.lang.Float eval = (
					java.lang.Float.parseFloat(t.get(47).toString())+ java.lang.Float.parseFloat(t.get(42).toString())+
					java.lang.Float.parseFloat(t.get(49).toString())+ java.lang.Float.parseFloat(t.get(20).toString())+
					java.lang.Float.parseFloat(t.get(50).toString())+ java.lang.Float.parseFloat(t.get(26).toString())+
					java.lang.Float.parseFloat(t.get(35).toString())+ java.lang.Float.parseFloat(t.get(18).toString())+
					java.lang.Float.parseFloat(t.get(32).toString())+ java.lang.Float.parseFloat(t.get(37).toString())+
					java.lang.Float.parseFloat(t.get(38).toString())+ java.lang.Float.parseFloat(t.get(3).toString())) / 12;


			java.lang.Float motivation = (
					java.lang.Float.parseFloat(t.get(41).toString())+ java.lang.Float.parseFloat(t.get(22).toString())+
					java.lang.Float.parseFloat(t.get(34).toString())+ java.lang.Float.parseFloat(t.get(39).toString())+
					java.lang.Float.parseFloat(t.get(27).toString())+ java.lang.Float.parseFloat(t.get(55).toString())+
					java.lang.Float.parseFloat(t.get(6).toString())+ java.lang.Float.parseFloat(t.get(8).toString())) / 8;

			java.lang.Float mental_ability = (
					java.lang.Float.parseFloat(t.get(54).toString())+ java.lang.Float.parseFloat(t.get(21).toString())+
					java.lang.Float.parseFloat(t.get(52).toString())+ java.lang.Float.parseFloat(t.get(46).toString())+
					java.lang.Float.parseFloat(t.get(28).toString())+ java.lang.Float.parseFloat(t.get(5).toString())+
					java.lang.Float.parseFloat(t.get(0).toString())+ java.lang.Float.parseFloat(t.get(44).toString())+
					java.lang.Float.parseFloat(t.get(29).toString()) ) / 9;

			java.lang.Float team_cohesion = (
					java.lang.Float.parseFloat(t.get(13).toString())+ java.lang.Float.parseFloat(t.get(36).toString())+
					java.lang.Float.parseFloat(t.get(17).toString())+ java.lang.Float.parseFloat(t.get(25).toString())+
					java.lang.Float.parseFloat(t.get(53).toString())+ java.lang.Float.parseFloat(t.get(24).toString()) ) / 6;
			String res_str = t.get(7).toString();
			Tuple5<Tuple2<String,Float>,Tuple2<String,Float>,Tuple2<String,Float>,Tuple2<String,Float>,Tuple2<String,Float>> res_stat =
					new Tuple5<Tuple2<String,Float>,Tuple2<String,Float>,Tuple2<String,Float>,Tuple2<String,Float>,Tuple2<String,Float>>(
							new Tuple2<String, Float>("Stress Control",stress),
							new Tuple2<String, Float>("Performance under evaluation",eval),
							new Tuple2<String, Float>("Motivation",motivation),
							new Tuple2<String, Float>("Mental ability",mental_ability),
							new Tuple2<String, Float>("Team Cohesion",team_cohesion));
			return new Tuple2<String,Tuple5<Tuple2<String, Float>,Tuple2<String, Float>,Tuple2<String, Float>,Tuple2<String, Float>,Tuple2<String, Float>>>(res_str,res_stat);
		});
		JavaPairRDD<String,Tuple5<Float,Float,Float,Float,Float>> playerStatistics = playerStatsRDD.mapToPair(t -> {
			Tuple5<Float,Float,Float,Float,Float> res_stat =
					new Tuple5<Float,Float,Float,Float,Float>(
							t._2()._1()._2,
							t._2()._2()._2,
							t._2()._3()._2,
							t._2()._4()._2,
							t._2()._5()._2
					);
			return new Tuple2<String,Tuple5<Float,Float,Float,Float,Float>>(t._1,res_stat);
		});
		JavaRDD<String> csv_valRDD = playerStatistics.map(t -> {
			String temp = t._1 + "," + t._2._1().toString() + "," + t._2._2().toString() + "," +
					t._2._3().toString() + "," + t._2._4().toString() + "," + t._2._5().toString();
			return temp;
		});
		JavaRDD<String> colnamesRDD = ctx.textFile("colnames.txt");
		JavaRDD<String> readyCSV = colnamesRDD.union(csv_valRDD);
		JavaRDD<String> temp = readyCSV.coalesce(1);
		List<String> CSV_players = temp.collect();

		String final_str = "";
		for(int i = 0; i<CSV_players.size();i++){
			final_str = final_str + CSV_players.get(i);
			if(i!=CSV_players.size()-1){
				final_str = final_str + "\n";
			}
		}

		FileWriter writer = new FileWriter("players_test_aggregations.csv");
		writer.write(final_str);

		writer.close();


		return out;
	}
}

