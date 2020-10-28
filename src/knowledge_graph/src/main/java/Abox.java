import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDF;
import org.apache.commons.compress.compressors.snappy.SnappyCompressorInputStream;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;

public class Abox {

    public static void transformDemographics() throws IOException {

        Model model = ModelFactory.createDefaultModel();
        // read Papers from Edition csv
        BufferedReader csvReader = new BufferedReader(new FileReader(config.demographics_input));
        // skip the header line
        csvReader.readLine();
        String row;
        while ((row = csvReader.readLine()) != null) {
            String[] row_data = row.split(",");

            String age = row_data[0].replace("/", "_");
            String clubName = row_data[11].replace("\"", "");
            String bdate = row_data[12].replace("\"", "");
            String division = row_data[13].replace("\"", "");
            String full_name = row_data[14].replace("\"", "");
            String hairstyle = row_data[15].replace("\"", "");
            String height_cm = row_data[16].replace("\"", "");
            String league = row_data[17].replace("\"", "");
            String nation = row_data[18].replace("\"", "");
            String position = row_data[19].replace("\"", "");
            String preferred_foot = row_data[20].replace("\"", "");
            String rating = row_data[21].replace("\"", "");
            String squad_number = row_data[22].replace("\"", "");
            String weight_kg = row_data[23].replace("\"", "");
            //String county_league = row_data[25].replace("\"", "");

            String playerUri = full_name.replace(" ", "_");
            Resource player = model.createResource(config.BASE_URL + playerUri)
                    .addProperty(model.createProperty(RDF.type.toString()), model.createResource(config.BASE_URL+"Football_player"))
                    .addProperty(model.createProperty(config.BASE_URL + "full_name"), full_name)
                    .addProperty(model.createProperty(config.DBO + "age"), model.createTypedLiteral(Integer.parseInt(age)))
                    .addProperty(model.createProperty(config.BASE_URL + "birth_date"), bdate)
                    .addProperty(model.createProperty(config.BASE_URL + "division"), model.createTypedLiteral(Integer.parseInt(division)))
                    .addProperty(model.createProperty(config.BASE_URL + "hairstyle"), hairstyle)
                    .addProperty(model.createProperty(config.BASE_URL + "height_cm"), height_cm)
                    .addProperty(model.createProperty(config.BASE_URL + "league"), league)
                    .addProperty(model.createProperty(config.DBO + "nation"), nation)
                    .addProperty(model.createProperty(config.BASE_URL + "preferred_foot"), preferred_foot)
                    .addProperty(model.createProperty(config.BASE_URL + "rating"), model.createTypedLiteral(Integer.parseInt(rating)))
                    .addProperty(model.createProperty(config.BASE_URL + "squad_number"), squad_number)
                    .addProperty(model.createProperty(config.BASE_URL + "weight_kg"), weight_kg)
                    //.addProperty(model.createProperty(config.BASE_URL + "county_league"), county_league)
                    .addProperty(model.createProperty(config.BASE_URL + "position"), position);

            String clubUri = clubName.replace(" ", "_");
            Resource club = model.createResource(config.DBR + clubUri)
                    .addProperty(model.createProperty(RDF.type.toString()), model.createResource(config.DBO+"SoccerClub"));


            Resource has_volume = model.createResource(config.BASE_URL+playerUri)
                    .addProperty(model.createProperty(config.BASE_URL+"plays_in"), model.createResource(config.DBR+clubUri));
        }
        csvReader.close();

        // write the mode to file
        model.write(new PrintStream(
                new BufferedOutputStream(
                        new FileOutputStream(config.OUTPUT_FILE_PATH+"players_demographics.nt")), true), "NT");
    }

    public static void transformFacebookData() throws IOException {
        Model model = ModelFactory.createDefaultModel();
        BufferedReader csvReader = new BufferedReader(new FileReader(config.facebook_input));
        csvReader.readLine();

        String row;
        HashMap<String, Boolean> facebook_accounts = new HashMap<String, Boolean>();
        Property typeProperty = model.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

        while ((row = csvReader.readLine()) != null) {
            String[] row_data = row.split(",");

            String full_name = row_data[0];
            String facebook_id = row_data[1];
            String favorite_team = row_data[2];
            String favorite_team_id = row_data[3];
            String favorite_athlete = row_data[4];
            String favorite_athlete_id = row_data[5];

            String playerUri = full_name.replace(" ", "_");

            Resource facebook_account = null;
            Resource player = null;
            if (!facebook_accounts.containsKey(facebook_id)) {
                facebook_accounts.put(facebook_id, Boolean.TRUE);
                facebook_account = model.createResource(config.BASE_URL + facebook_id)
                        .addProperty(typeProperty, model.getResource(config.BASE_URL + "Account_FB"))
                        .addProperty(model.getProperty(config.BASE_URL + "social_network_fb"), config.DBR + "Facebook");
                player = model.getResource(config.BASE_URL + playerUri)
                        .addProperty(model.createProperty(config.BASE_URL + "has_account_fb"), facebook_account);

            } else {
                facebook_account = model.getResource(config.BASE_URL + facebook_id);
                player = model.getResource(config.BASE_URL + playerUri);
            }

            if (favorite_athlete.equals("null")) {
                Resource team_facebook_account = null;
                Resource team = null;
                if (!facebook_accounts.containsKey(favorite_team_id)) {
                    team_facebook_account = model.createResource(config.BASE_URL + favorite_team_id)
                            .addProperty(typeProperty, model.getResource(config.BASE_URL + "Account_FB"));
                    team = model.getResource(config.DBR + favorite_team.replace(" ", "_"))
                            .addProperty(model.createProperty(config.BASE_URL + "has_account_fb"), team_facebook_account);
                } else {
                    team_facebook_account = model.getResource(config.BASE_URL + favorite_team_id);
                    team = model.getResource(config.DBR + favorite_team.replace(" ", "_"));
                }
                facebook_account.addProperty(model.createProperty(config.BASE_URL + "has_favorite_team"), team_facebook_account);
            } else {
                Resource athlete = null;
                Resource athlete_facebook_account = null;
                if (!facebook_accounts.containsKey(favorite_athlete_id)) {
                    athlete_facebook_account = model.createResource(config.BASE_URL + favorite_athlete_id)
                            .addProperty(typeProperty, model.getResource(config.BASE_URL + "Account_FB"));
                    athlete = model.createResource(config.DBR + favorite_athlete.replace(" ", "_"))
                            .addProperty(typeProperty, model.getResource(config.DBO + "Athlete"))
                            .addProperty(model.createProperty(config.BASE_URL + "has_account_fb"), athlete_facebook_account);
                } else {
                    athlete_facebook_account = model.getResource(config.BASE_URL + favorite_athlete_id);
                    athlete = model.getResource(config.DBR + favorite_athlete.replace(" ", "_"));
                }
                facebook_account.addProperty(model.createProperty(config.BASE_URL + "has_favorite_athlete"), athlete_facebook_account);
            }
        }
        csvReader.close();

        // write the mode to file
        model.write(new PrintStream(
                new BufferedOutputStream(
                        new FileOutputStream(config.OUTPUT_FILE_PATH+"players_favorite_athletes_and_teams.nt")), true), "NT");
    }
    
    public static void transformTests() throws IOException {

        Model model = ModelFactory.createDefaultModel();

        BufferedReader csvReader = new BufferedReader(new FileReader(config.test_input));
        // skip the header line
        csvReader.readLine();
        String row;

        while ((row = csvReader.readLine()) != null) {
            String[] row_data = row.split(",");

            // Attributes to add
            // Fullname
            String full_name = row_data[0];
            // stress_control
            String stress_Control = row_data[1];
            // Performance Under Evaluation
            String Performance_under_evaluation = row_data[2];
            // Motivation
            String Motivation = row_data[3];
            // Mental Ability
            String Mental_ability = row_data[4];
            // Team Cohesion
            String Team_Cohesion = row_data[5];

            // this should be corrected with cprd_tes_uri_player_name
            String playerUri = full_name.replace(" ", "_");
            String testUri = "cprd_test_" + playerUri;
            Resource test = model.createResource(config.BASE_URL  + testUri)
                    .addProperty(model.createProperty(RDF.type.toString()), model.createResource(config.BASE_URL+"CPRD_Test"))
                    .addProperty(model.createProperty(config.BASE_URL + "Team_Cohesion"), Team_Cohesion)
                    .addProperty(model.createProperty(config.BASE_URL + "Mental_ability"), Mental_ability)
                    .addProperty(model.createProperty(config.BASE_URL + "Motivation"), Motivation)
                    .addProperty(model.createProperty(config.BASE_URL + "Performance_under_evaluation"), Performance_under_evaluation)
                    .addProperty(model.createProperty(config.BASE_URL + "Stress_Control"), stress_Control);
            Property takes = model.createProperty(config.BASE_URL,"takes");
            model.add(model.getResource(config.BASE_URL + playerUri), takes, model.getResource(config.BASE_URL + testUri));

        }
        csvReader.close();

        // write the mode to file
        model.write(new PrintStream(
                new BufferedOutputStream(
                        new FileOutputStream(config.OUTPUT_FILE_PATH+"players_test.nt")), true), "NT");

    }
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

                Resource playerModel = model.createResource(playerUri);
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
