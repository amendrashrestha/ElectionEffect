package main;

import model.Database;
import utilities.IOProperties;
import utilities.IOReadWrite;
import static utilities.IOReadWrite.getListOfFile;
import java.io.File;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author amendrashrestha
 */
public class CreateFeatureVector {

    public static void create(String newsType, String liwcCountFile, String FVfolderPath)  {

        try {
            String tableName;
            if (newsType.contains("-")) {
                tableName = "tbl_count_" + newsType.replaceAll("-", "");
            } else {
                tableName = "tbl_count_" + newsType.replaceAll("\\s+", "");
            }
            Database.dropTable(tableName);
            Database.createTable(tableName);
            
            Database.dropTable("tbl_category");
            Database.createCategoryTable("tbl_category");
            Database.insertIntoCategory("tbl_category", liwcCountFile);
            
//            IOReadWrite.createFileWithHeader(folderPath + "FeatureVector.csv");
            File FeatVectFile = new File(FVfolderPath);
            
            if (!FeatVectFile.exists()) {
                IOReadWrite.createFileWithHeader(FeatVectFile, liwcCountFile);
            }
            
            ArrayList<String> files = IOReadWrite.getListOfFile(liwcCountFile);
            
            for (String file : files) {
                if (!file.startsWith(".")) {
                    Database.loadFileIntoTables(liwcCountFile + "/" + file, tableName, "0");
//                Database.loadFileIntoTables(file, tableName, "0");
                }
            }
            Database.addIndexToTable(tableName);
            returnCount(FVfolderPath, tableName, liwcCountFile);
            
            System.out.println("FV Process Completed.....");
//        }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IOException | SQLException ex) {
            Logger.getLogger(CreateFeatureVector.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void returnCount(String filename, String tableName, String wordCountFiles) throws IOException {
        ArrayList<String> featureCount = Database.getDateWithCount(tableName);

        if (featureCount.size() > 0) {
            ArrayList<String> featToRemove = new ArrayList();

            List<String> totalFeature = getListOfFile(wordCountFiles);

            for (int i = 0; i < totalFeature.size(); i++) {
                if (totalFeature.get(i).startsWith(".")) {
                    totalFeature.remove(i);
                    break;
                }
            }

            int totalFeat = totalFeature.size();

            for (int i = 0; i <= featureCount.size(); i++) {
                if (i < totalFeat) {
//                    System.out.println(i + "-->" + featureCount.get(i));
                    featToRemove.add(featureCount.get(i));
                } else {
                    IOReadWrite.ExtractDateandCount(featToRemove, filename);
                    featureCount.removeAll(featToRemove);

                    i = -1;
                    if (featureCount.size() <= 0) {
                        break;
                    }
                    featToRemove.clear();
                }
            }
        }
    }

    static void loadUserPeriodFile(String FVfolderPath) {
        try {
            String tableName = "tbl_count_user_period_category";

            Database.dropTable(tableName);
            Database.createPeriodTable(tableName);
            Database.loadPeriodFileInTables(FVfolderPath, tableName);

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(CreateFeatureVector.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    static void loadUserCountFile(String FVfolderPath, String category, String ignoreLine) {
        try {
            String tableName = "tbl_count_user_trump";

            Database.dropTable(tableName);
            Database.createCountTable(tableName);
            Database.loadFileIntoTables(FVfolderPath, tableName, ignoreLine);

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(CreateFeatureVector.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    static void countChange(String categoryFiles) {
        try {
            String tableName = "tbl_user_year";
            String from_year = "2007";
            String to_year = "2014";
            String filename = "valid_user_period_diff.tsv";
            File folderPath = new File(IOProperties.USER_PERIOD_COUNT_FILEPATH + "/" + filename);

            IOReadWrite.createFileWithHeader(folderPath, categoryFiles);

            List<String> users = Database.getUsers(tableName, from_year, to_year);

            for (String user : users) {
                if (user.length() != 0) {
                    Database.getDifference(user, folderPath);
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(CreateFeatureVector.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    static void createANOVAFile(String FVfolderPath, String ANOVAFileFolderPath, String single_newsType) {
        String dateTableName = "tbl_dates";
        String tableName = "tbl_user_ANOVA";
        String tableEventDate = "tbl_event_date";
        String eventDate = "2016-11-08"; // 2016 election result day
        int days = 2;
        int period_size = 7;
        int no_of_weeks = 4;

        String ANOVAFolderPath = ANOVAFileFolderPath + single_newsType;
        
        Database.createANOVATable(tableName);
        Database.loadANOVAFileIntoTables(FVfolderPath, tableName);
        
        // creating table with all possible dates
        Database.createDateTable(dateTableName);
        
        Database.createEventDateTable(tableEventDate, eventDate, single_newsType);

        List<String> validUser = Database.getUsers(tableName);
//        List<String> validUser = new ArrayList();
//        validUser.add("14words_of_truth");

        List<String> categories = Database.getCategories();
        
        IOReadWrite.createANOVAForumDirectory(ANOVAFolderPath);

        for (String category : categories) {
            try {
                File categoryFile = new File(ANOVAFolderPath + "/" + category + ".csv");
                IOReadWrite.createANOVAFileWithHeader(categoryFile);
            } catch (IOException ex) {
                Logger.getLogger(CreateFeatureVector.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        for (String singleUser : validUser) {
            Database.createANOVAFVFile(eventDate, singleUser, period_size, days, no_of_weeks, ANOVAFolderPath, categories, validUser.size());
        }

    }

}
