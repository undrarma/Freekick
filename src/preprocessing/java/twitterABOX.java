import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDF;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class twitterABOX {
    public static void generateTwitterABOX() throws FileNotFoundException {
        Model model = ModelFactory.createDefaultModel();
        String url = "jdbc:postgresql://localhost:5432/Freekick";
        String user = "postgres";
        String password = "admin";
        try (Connection con = DriverManager.getConnection(url, user, password);
             PreparedStatement pst = con.prepareStatement("SELECT tM.\"twitterAcc\", tM.\"playerURI\", tS.\"sentimentScore\"\n" +
                     "FROM info.\"twitterMap\" tM\n" +
                     "LEFT JOIN info.\"twitter\" tS\n" +
                     "\tON tM.\"twitterAcc\" = tS.\"twitterAcc\";");
             ResultSet rs = pst.executeQuery()) {
            while (rs.next()) {
                String twitterAcc = rs.getString(1);
                String playerUri = rs.getString(2);
                int playerSent = rs.getInt(3);

                Resource twitterModel = model.createResource(config.BASE_URL + twitterAcc)
                        .addProperty(model.createProperty(RDF.type.toString()), model.createResource(config.BASE_URL+"Account_TW"))
                        .addProperty(model.createProperty(config.BASE_URL + "social_network_tw"), model.createResource(config.DBR + "Twitter"))
                        .addLiteral(model.createProperty(config.BASE_URL + "sentimentScore"), playerSent);

                Resource playerModel = model.createResource(config.BASE_URL + playerUri);
                Property has_account = model.getProperty(config.BASE_URL + "has_account_tw");
                model.add(model.createStatement(playerModel, has_account, twitterModel));

            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        model.write(new PrintStream(
                new BufferedOutputStream(
                        new FileOutputStream("src\\main\\resources\\twitter.nt")), true), "NT");
//        Resource player = model.createResource(config.BASE_URL + playerUri)
    }
}
