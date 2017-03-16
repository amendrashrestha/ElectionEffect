package main;

import java.io.File;
import model.Database;
import utilities.IOReadWrite;
import utilities.IOProperties;

/**
 *
 * @author amendrashrestha
 */
public class main {

    public static void init() {
        String single_newsType = "vnnforum";
        String rankTable = "tbl_user_rank";
        String countTable = "tbl_count_" + single_newsType; 
        
//        String FV_news_table = "tbl_count_" + single_newsType;
        String postTableName = "tbl_" + single_newsType + "_posts_user_day";
        String validUserTable = "tbl_" + single_newsType + "_valid_user";

        String LimitedLIWCFeatureFile = IOProperties.LIWC_LIMITED_CATEGORY_WORD_FILEPATH;
        String LIWCCountFolderPath = IOProperties.LIWC_COUNT_FILEPATH;
        String FVfolderPath = IOProperties.FEATURE_VECTOR_FILEPATH + "FeatureVector_" + single_newsType + ".csv";

        String ANOVAfolderPath = IOProperties.ANOVA_FEATURE_VECTOR_FILEPATH;

        String liwcCountFilePath = LIWCCountFolderPath + "/" + single_newsType;

        File liwcCountFile = new File(liwcCountFilePath);
        
        // Getting Trump valid users and non-trump users
        Database.createValidUserTable(validUserTable, postTableName);
        IOReadWrite.categoryCount(LimitedLIWCFeatureFile, liwcCountFile, postTableName, validUserTable);
        Database.loadCountFileIntoTables(liwcCountFilePath + "/Trump", countTable);
        Database.createValidTrumpUser(validUserTable, countTable);
        Database.createNonTrumpUser(postTableName, validUserTable, countTable);

//        System.out.println("\n Counting Noun in " + single_newsType + " ......");
//        IOReadWrite.countNoun(liwcCountFile, postTableName, validUserTable);
          
//        System.out.println("\n Counting words in " + single_newsType + " ......");
//        IOReadWrite.categoryCount(LimitedLIWCFeatureFile, liwcCountFile, postTableName, validUserTable);
//
//        System.out.println("\n Creating FV for " + single_newsType + " ......");
//        CreateFeatureVector.create(single_newsType, liwcCountFilePath, FVfolderPath);

//        System.out.println("Creating Groups table ....");
//        Database.createRankTable(rankTable, FV_news_table);
//
//        System.out.println("Creating ANOVA files....");
//        CreateFeatureVector.createANOVAFile(FVfolderPath, ANOVAfolderPath, single_newsType);
    }

    public static void main(String args[]) {
        init();
    }

}
