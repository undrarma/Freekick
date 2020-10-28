import java.io.*;
import java.nio.Buffer;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import javax.json.JsonArray;

public class createURIs {
    public static void storeSentimentsCount() {
        JSONParser parser = new JSONParser();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter("src\\main\\resources\\playersURI.csv"))) {
            StringBuilder sb = new StringBuilder();
            try {
                Object obj = parser.parse(new FileReader("src/main/resources/soccer_england_div_1_2_3_50_players.json"));
                JSONArray jsonArray = (JSONArray) obj;

                Iterator<JSONObject> iterator = jsonArray.iterator();
                while (iterator.hasNext()) {
                    JSONObject row = iterator.next();
                    String full_name = row.get("Full_Name").toString();
                    String playerUri = full_name.replace(" ", "_");
                    String IRI = "http://www.semanticweb.org/ontology/football/" + playerUri + "\n";
                    sb.append(IRI);
                    //System.out.println(IRI);
                }
                bw.write(sb.toString());
                bw.close();
            } catch (Exception e)   {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
