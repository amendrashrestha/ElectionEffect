package model;

import utilities.IOReadWrite;
import static utilities.IOReadWrite.getListOfFile;
import java.io.File;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import static utilities.IOReadWrite.writeIntoFile;

/**
 *
 * @author amendrashrestha
 */
public class Database {

    public static void dropTable(String tableName) throws
            ClassNotFoundException, InstantiationException, IllegalAccessException {
        try (Connection connection = Connect.getConn()) {
            String dropQuery = "drop table if exists " + tableName;
            Statement statement = connection.createStatement();
            statement.executeUpdate(dropQuery);
        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void createTable(String tableName) throws
            ClassNotFoundException, InstantiationException, IllegalAccessException {
        try (Connection connection = Connect.getConn()) {
            String dropTableQuery = "DROP TABLE IF EXISTS " + tableName;
            String createQuery = "CREATE table " + tableName + " (User varchar(100), "
                    + "Count int, Category varchar(100), Post_Size int)";
//            System.out.println(createQuery);
            Statement statement = connection.createStatement();
//            System.out.println(createQuery);
            statement.executeUpdate(dropTableQuery);
            statement.executeUpdate(createQuery);

        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void createPeriodTable(String tableName) throws
            ClassNotFoundException, InstantiationException, IllegalAccessException {
        try (Connection connection = Connect.getConn()) {
            String dropTableQuery = "DROP TABLE IF EXISTS " + tableName;
            String createQuery = "CREATE table " + tableName + " (ID int primary key auto_increment, User varchar(100), "
                    + "achieve	double	, adverb	double	, affect	double	, anger	double	, anx	double	, "
                    + "article	double	, assent	double	, auxverb	double	, bio	double	, body	double	, "
                    + "cause	double	, certain	double	, cogmech	double	, conj	double	, death	double	, "
                    + "discrep	double	, Eliten	double	, excl	double	, family	double	, feel	double	, "
                    + "Folket	double	, Folkets_företrädare	double	, friend	double	, funct	double	, "
                    + "future	double	, health	double	, hear	double	, home	double	, humans	double	, "
                    + "i	double	, incl	double	, ingest	double	, inhib	double	, insight	double	, "
                    + "ipron	double	, leisure	double	, Minoriteterna	double	, money	double	, motion	double	, "
                    + "negate	double	, negemo	double	, nonfl	double	, number	double	, past	double	, "
                    + "percept	double	, posemo	double	, ppron	double	, preps	double	, present	double	, "
                    + "pronoun	double	, quant	double	, relativ	double	, relig	double	, sad	double	, "
                    + "see	double	, sexual	double	, shehe	double	, social	double	, space	double	, "
                    + "swear	double	, tentat	double	, they	double	, time	double	, verb	double	, we	double	, "
                    + "work	double	, you	double	)";
//            System.out.println(createQuery);
            Statement statement = connection.createStatement();
//            System.out.println(createQuery);
            statement.executeUpdate(dropTableQuery);
            statement.executeUpdate(createQuery);

        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void createCountTable(String tableName) throws
            ClassNotFoundException, InstantiationException, IllegalAccessException {
        try (Connection connection = Connect.getConn()) {
            String dropTableQuery = "DROP TABLE IF EXISTS " + tableName;
            String createQuery = "CREATE table " + tableName + " (User varchar(100), Date varchar(20), Category double)";
//            System.out.println(createQuery);
            Statement statement = connection.createStatement();
//            System.out.println(createQuery);
            statement.executeUpdate(dropTableQuery);
            statement.executeUpdate(createQuery);

        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void loadFileIntoTables(String filepath, String tableName, String ignoreLine) throws
            ClassNotFoundException, InstantiationException, IllegalAccessException {
        try (Connection connection = Connect.getConn()) {
//            String truncateQuery = "truncate table tbl_featVector";
            String query = "LOAD DATA LOCAL INFILE " + "\"" + filepath + "\"" + "\n"
                    + "INTO TABLE " + tableName + "\n"
                    + "COLUMNS TERMINATED BY ','\n"
                    + "LINES TERMINATED BY '\n'\n"
                    + "IGNORE " + ignoreLine + " LINES;";

//            System.out.println(query);
            Statement statement = connection.createStatement();
//            statement.executeUpdate(truncateQuery);
            statement.executeUpdate(query);

        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void loadPeriodFileInTables(String filepath, String tableName) throws
            ClassNotFoundException, InstantiationException, IllegalAccessException {
        try (Connection connection = Connect.getConn()) {
//            String truncateQuery = "truncate table tbl_featVector";
            String query = "LOAD DATA INFILE " + "\"" + filepath + "\"" + "\n"
                    + "INTO TABLE " + tableName + "\n"
                    + "COLUMNS TERMINATED BY ','\n"
                    + "LINES TERMINATED BY '\\n'\n"
                    + "IGNORE 1 LINES \n"
                    + "(User, achieve	, adverb	, affect	, anger	, anx	, "
                    + "article	, assent	, auxverb	, bio	, body	, cause	, "
                    + "certain	, cogmech	, conj	, death	, discrep	, Eliten	, "
                    + "excl	, family	, feel	, Folket	, Folkets_företrädare	, "
                    + "friend	, funct	, future	, health	, hear	, home	, humans	, "
                    + "i	, incl	, ingest	, inhib	, insight	, ipron	, leisure	, "
                    + "Minoriteterna	, money	, motion	, negate	, negemo	, nonfl	, "
                    + "number	, past	, percept	, posemo	, ppron	, preps	, present	, "
                    + "pronoun	, quant	, relativ	, relig	, sad	, see	, sexual	, shehe	, "
                    + "social	, space	, swear	, tentat	, they	, time	, verb	, we	, work	, you	);";

//            System.out.println(query);
            Statement statement = connection.createStatement();
//            statement.executeUpdate(truncateQuery);
            statement.executeUpdate(query);

        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void addIndexToTable(String tableName) throws
            ClassNotFoundException, InstantiationException, IllegalAccessException {
        try (Connection connection = Connect.getConn()) {
//            String dropTableQuery = "DROP TABLE " +  tableName;
            String indexQuery = "ALTER table " + tableName + " ADD"
                    + " INDEX idx_" + tableName + " USING BTREE (Category, User)";
//            System.out.println(createQuery);
            Statement statement = connection.createStatement();
            statement.executeUpdate(indexQuery);

        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void createCategoryTable(String tableName) {
        try (Connection connection = Connect.getConn()) {
            String dropTableQuery = "DROP TABLE IF EXISTS " + tableName;
            String createQuery = "CREATE table " + tableName + " (Category varchar(50))";
            String indexQuery = "ALTER table " + tableName
                    + " add index idx_cat(Category)";
//            System.out.println(createQuery);
            Statement statement = connection.createStatement();
//            System.out.println(createQuery);
            statement.executeUpdate(dropTableQuery);
            statement.executeUpdate(createQuery);
            statement.executeUpdate(indexQuery);
        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void insertIntoCategory(String tableName, String wordCountFiles) throws IOException, SQLException {
        PreparedStatement preparedStatement;
        try {
            Connection connection = Connect.getConn();
            List<String> totalFeature = getListOfFile(wordCountFiles);

            String insertQuery = "INSERT INTO " + tableName + " (Category) "
                    + "VALUES (?)";
//            System.out.println(createQuery);
            preparedStatement = connection.prepareStatement(insertQuery);

            for (String totalFeature1 : totalFeature) {
                if (!totalFeature1.startsWith(".")) {
                    preparedStatement.setString(1, totalFeature1.replace(".txt", ""));
                    preparedStatement.executeUpdate();
                }
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static ArrayList getDateWithCount(String tableName) {
        ArrayList<String> dateAndResult = new ArrayList();
        try (Connection connection = Connect.getConn()) {
            String dropTempTable1 = "DROP TEMPORARY TABLE IF EXISTS table1";
            String dropTempTable2 = "DROP TEMPORARY TABLE IF EXISTS table2";

            String createTemTable1 = "CREATE TEMPORARY TABLE IF NOT EXISTS table1 AS "
                    + "(select T1.User, category from (select distinct User from " + tableName
                    //                    + " where user like " + "\'" + "%1" + "\'" + " || " + "user like " + "\'" + "%2" + "\'"
                    + ") T1, "
                    + "tbl_category T2 "
                    //                    + "tbl_category T2 where T2.category = 500 || T2.category = 503 "
                    + "order by User, Category);";
            String createTemTable2 = "CREATE TEMPORARY TABLE IF NOT EXISTS table2 AS "
                    + "(select (sum(count))/sum(Post_Size) as Total, User, Category from " + tableName
                    //                    + " where user like " + "\'" + "%1" + "\'" + " || " + "user like " + "\'" + "%2" + "\'"
                    + " group by User, Category\n"
                    + "order by User, Category);";
//              String createTemTable2 = "CREATE TEMPORARY TABLE IF NOT EXISTS table2 AS "
//                    + "(select abs(1/(LOG(Count/Post_Size))) as Total, User, Category from " + tableName
////                    + " where user like " + "\'" + "%1" + "\'" + " || " + "user like " + "\'" + "%2" + "\'"
//                    + " group by User, Category\n"
//                    + "order by User, Category);";

            String addIndexTable1 = "ALTER TABLE table1 ADD INDEX idx_tbl1(User, Category);";
            String addIndexTable2 = "ALTER TABLE table2 ADD INDEX idx_tbl2(User, Category);";

            String resultQuery = "select R1.User, ifnull(R2.Total,0) as Total from table1 R1 \n"
                    + "LEFT JOIN \n"
                    + "table2 R2\n"
                    + "ON R1.Category = R2.category and R1.User = R2.User\n"
                    + "order by 1, R1.Category;";

            Statement statement = connection.createStatement();

            statement.executeUpdate(dropTempTable1);
            statement.executeUpdate(dropTempTable2);
            statement.executeUpdate(createTemTable1);
            statement.executeUpdate(createTemTable2);

            statement.executeUpdate(addIndexTable1);
            statement.executeUpdate(addIndexTable2);

            ResultSet result = statement.executeQuery(resultQuery);
            while (result.next()) {
                String date = result.getString(1);
                double count = result.getDouble(2);
                dateAndResult.add(date + "," + count);

            }
//            System.out.println("---------");

        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
        return dateAndResult;
    }

    public static ArrayList getFVOfForum(String forumName) {
        ArrayList<Double> categoryCount = new ArrayList();
        try (Connection connection = Connect.getConn()) {

            String selectTable = "SELECT (sum(count) * 100)/sum(Post_Size) as Total from tbl_count_" + forumName
                    + " group by Category"
                    + " order by Category;";

            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(selectTable);

            while (result.next()) {
                double count = result.getDouble(1);
                categoryCount.add(count);
            }

        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
        return categoryCount;
    }

    public static List<String> getPosts(String tableName) {
        List<String> news = new ArrayList();
        try {
            Connection connection = Connect.getConn();
            String sizeQuery = "SET group_concat_max_len = 100000000000000000;";

            String selectQuery = "SELECT News from " + tableName;

            Statement statement = connection.createStatement();
            statement.execute(sizeQuery);
            ResultSet result = statement.executeQuery(selectQuery);
            while (result.next()) {
                String post = result.getString("News");
                news.add(post);
            }
        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
        return news;
    }

    public static void createExtremeWordCountTable(String tableName) {
        try (Connection connection = Connect.getConn()) {
            String dropTableQuery = "DROP TABLE IF EXISTS " + tableName;
            String createQuery = "CREATE table " + tableName + " (Category varchar(100))";
//            System.out.println(createQuery);
            Statement statement = connection.createStatement();
//            System.out.println(createQuery);
            statement.executeUpdate(dropTableQuery);
            statement.executeUpdate(createQuery);
        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void writeExtremeWordCount(String forumName, File folderPath) {

        try (Connection connection = Connect.getConn()) {

            String selectTable = "SELECT Category, count(*) as Total from tbl_extreme_word_" + forumName
                    + " group by Category"
                    + " order by Category;";

            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(selectTable);

            while (result.next()) {
                String category = result.getString(1);
                String count = result.getString(2);
                String columnToWrite = (category + " \t " + count);
                IOReadWrite.writeWordsIntoFile(columnToWrite, folderPath);
            }

        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException | IOException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static List<String> getUsers(String tableName, String from_year, String to_year) {
        List<String> users = new ArrayList();
        try {
            Connection connection = Connect.getConn();
            String selectQuery = "select distinct T1.Year_" + from_year
                    + " from " + tableName + " T1 "
                    + "INNER JOIN " + tableName
                    + " T3 on T1.Year_" + from_year
                    + " = T3.Year_" + to_year;

            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(selectQuery);
            while (result.next()) {
                String user = result.getString(1);
                users.add(user);
            }
        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
        return users;
    }

    public static List<String> getPowerUsers(String tableName) {
        List<String> users = new ArrayList();
        try {
            Connection connection = Connect.getConn();
            String selectQuery = "select distinct User from tbl_power_user";

//            String selectQuery = "select distinct User from  " + tableName
//                    + " where User not in "
//                    + "(select distinct User from tbl_power_user)"; 
            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(selectQuery);
            while (result.next()) {
                String user = result.getString(1);
                users.add(user);
            }
        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
        return users;
    }

    public static List<String> getTrumpUsers(String tableName) {
        List<String> users = new ArrayList();
        try {
            Connection connection = Connect.getConn();
//            String selectQuery = "select User from "
//                    + tableName +
//                    " group by User\n" +
//                    "having min(Post_Date) >=  DATE_SUB(\"2016-11-08\", INTERVAL 45 DAY) \n" +
//                    "AND max(Post_Date) <= DATE_ADD(\"2016-11-08\" ,INTERVAL 45 DAY)";

            String selectQuery = "SELECT distinct User from " + tableName
                    + " WHERE Post_Date BETWEEN DATE_SUB(\"2016-11-08\", INTERVAL 45 DAY) "
                    + " AND DATE_ADD(\"2016-11-08\" ,INTERVAL 45 DAY)\n"
                    + " AND (User not like '%news%'\n"
                    + " AND User not like '%cnn%')\n"
                    + " group by User\n"
                    + " having count(*) > 50;";

//            System.out.println(selectQuery);
            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(selectQuery);
            while (result.next()) {
                String user = result.getString(1);
                users.add(user);
            }
        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
        return users;
    }

    public static List<String> getUsers(String tableName) {
        List<String> users = new ArrayList();
        try {
            Connection connection = Connect.getConn();
            String selectQuery = "SELECT distinct User from " + tableName;

            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(selectQuery);
            while (result.next()) {
                String user = result.getString(1);
                users.add(user);
            }
        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
        return users;
    }

    public static int returnValidUser(String user) throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
//        List<String> validUsers = new ArrayList();
        int validUser = 0;
        Connection connection = Connect.getConn();
        try {
//            String sizeQuery = "set group_concat_max_len = 100000000000;";
            String selectQuery = "call CheckRegularPostingUser('" + user + "', 10, 6);";
//            System.out.println(selectQuery);

            Statement statement = connection.createStatement();
//            statement.executeQuery(sizeQuery);
            ResultSet result = statement.executeQuery(selectQuery);

            while (result.next()) {
                validUser = result.getInt(1);
            }
        } catch (SQLException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            connection.close();
        }
        return validUser;
    }

    public static List<String> returnValidUserPost(String user) throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        List<String> posts = new ArrayList();
        Connection connection = Connect.getConn();
        try {
            String sizeQuery = "set group_concat_max_len = 100000000000;";
            String selectQuery = "call ReturnRegularPostingUserPost('" + user + "', 10, 6);";
//            System.out.println(selectQuery);

            Statement statement = connection.createStatement();
            statement.executeQuery(sizeQuery);
            ResultSet result = statement.executeQuery(selectQuery);

            while (result.next()) {
                String post = result.getString("Text");
                posts.add(post);
            }
        } catch (SQLException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            connection.close();
        }
        return posts;
    }

    public static String getPostsForUser(String tableName, String user, String year) throws SQLException {
//        List<String> news = new ArrayList();
        String post = "";
//        System.out.println(user);
        try (Connection connection = Connect.getConn()) {
            String sizeQuery = "SET group_concat_max_len = 100000000000000000;";

            String selectQuery = "SELECT GROUP_CONCAT(Text SEPARATOR ' ') as Text from " + tableName
                    + " where User = " + "\'" + user + "\'"
                    + " and substring(Post_Date,1,4) = " + "\'" + year + "\'"
                    + " group by substring(Post_Date,1,4);";

//            System.out.println(selectQuery);
            Statement statement = connection.createStatement();
            statement.execute(sizeQuery);
            ResultSet result = statement.executeQuery(selectQuery);

            while (result.next()) {
                post = result.getString(1);
//                news.add(post);
            }
        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
        return post;
    }

    public static List<String> getAllPostsForUser(String tableName, String user) throws SQLException {
        List<String> posts = new ArrayList();
        String post = "";
//        System.out.println(user);
        try (Connection connection = Connect.getConn()) {
            String sizeQuery = "SET group_concat_max_len = 100000000000000000;";

            String selectQuery = "SELECT Text from " + tableName
                    + " where User = " + "\'" + user + "\'";

//            System.out.println(selectQuery);
            Statement statement = connection.createStatement();
            statement.execute(sizeQuery);
            ResultSet result = statement.executeQuery(selectQuery);

            while (result.next()) {
                post = result.getString(1);
                posts.add(post);
            }
        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
        return posts;
    }

    public static void getPostsForUser(String tableName, String validUserTable, postFound postCallback) {

        try (Connection connection = Connect.getConn()) {
            String sizeQuery = "SET group_concat_max_len = 100000000000000000;";

            String selectQuery = "SELECT T1.User, Text from " + tableName + " T1 "
                    + "INNER JOIN " + validUserTable + " T2 "
                    + "ON T1.User = T2.User";

            Statement statement = connection.createStatement();
            statement.execute(sizeQuery);
            ResultSet result = statement.executeQuery(selectQuery);

            while (result.next()) {
                String user = result.getString(1);
                String post = result.getString(2);
                postCallback.onPostFound(user, post);
            }
        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void getDifference(String user, File folderPath) {

        try (Connection connection = Connect.getConn()) {

            String selectTable = "SELECT \n"
                    + "   g1.User, \n"
                    + "   (IF(g1.Exclusive > g2.Exclusive, \"Decrease\", \"Increase\")) AS Exclusive,\n"
                    + "   (IF(g1.I > g2.I, \"Decrease\", \"Increase\")) AS I,\n"
                    + "   (IF(g1.Inclusive > g2.Inclusive, \"Decrease\", \"Increase\")) AS Inclusive,\n"
                    + "   (IF(g1.Neg_Emo > g2.Neg_Emo, \"Decrease\", \"Increase\")) AS Neg_Emo,\n"
                    + "   (IF(g1.Pos_Emo > g2.Pos_Emo, \"Decrease\", \"Increase\")) AS Pos_Emo,\n"
                    + "   (IF(g1.They > g2.They, \"Decrease\", \"Increase\")) AS They\n"
                    + "FROM\n"
                    + "    tbl_count_user_period_category g1\n"
                    + "        INNER JOIN\n"
                    + "    tbl_count_user_period_category g2 ON g2.id = g1.id + 1    \n"
                    + "WHERE\n"
                    + "     g1.user like " + "\'" + user + "%\'" + "limit 1;";

            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(selectTable);

            while (result.next()) {
                String User = result.getString(1);
                String Exclusive = result.getString(2);
                String I = result.getString(3);
                String Inclusive = result.getString(4);
                String Neg_Emo = result.getString(5);
                String Pos_Emo = result.getString(6);
                String They = result.getString(7);
                String columnToWrite = (User + "\t" + Exclusive + "\t"
                        + I + "\t" + Inclusive + "\t" + Neg_Emo + "\t" + Pos_Emo
                        + "\t" + They);
                IOReadWrite.writeWordsIntoFile(columnToWrite, folderPath);
            }

        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException | IOException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static List<String> getPostsForYear(String textTableName, String single_year) throws SQLException {
//        String post = "";
        List<String> news = new ArrayList();
//        System.out.println(user);
        try (Connection connection = Connect.getConn()) {
            String sizeQuery = "SET group_concat_max_len = 1000000000000000;";

//            String selectQuery = "SELECT GROUP_CONCAT(Text SEPARATOR ' ') as Text from " + textTableName
//                    + " where substring(Post_Date,1,4) = " + "\'" + single_year + "\'"
//                    + " group by substring(Post_Date,1,4);";
            String selectQuery = "SELECT Text from " + textTableName
                    + " where Post_Date = " + "\'" + single_year + "\'";

//            System.out.println(selectQuery);
            Statement statement = connection.createStatement();
            statement.execute(sizeQuery);
            ResultSet result = statement.executeQuery(selectQuery);

            while (result.next()) {
                String post = result.getString(1);
                news.add(post);
            }
        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
        return news;
    }

    public interface postFound {

        void onPostFound(String user, String post);
    }

    public static void getPostsForDays(String textTableName, String from, String validUserTable, postFound postCallback) {
        try (Connection connection = Connect.getConn()) {
            String sizeQuery = "SET group_concat_max_len = 100000000000000000;";

            String selectQuery = "SELECT T1.User, T1.Post_Date, T1.Text from " + textTableName
                    + " T1 "
                    + "INNER JOIN " + validUserTable + " T2 "
                    //                    + "ON T1.User = " + "\'" + "@11C1P" + "\'"
                    + "ON T1.User = T2.User "
                    + "AND Post_Date BETWEEN DATE_SUB(\"2016-11-08\", INTERVAL 10 DAY) "
                    + "AND DATE_ADD(\"2016-11-08\" ,INTERVAL 10 DAY) "
                    //                    + "AND Text like '%trump%'"
                    + "group by T1.User, T1.Post_Date;";

//            System.out.println(selectQuery);
            Statement statement = connection.createStatement();
            statement.execute(sizeQuery);
            ResultSet result = statement.executeQuery(selectQuery);

            while (result.next()) {

                String user = result.getString(1);
                String date = result.getString(2);
                String post = result.getString(3);

                String user_date = user + " || " + date;
                postCallback.onPostFound(user_date, post);
//                news.put(user_date, post);
            }
        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
//        return news;
    }

    public static LinkedHashMap<String, String> getPostsForDays(String textTableName, String from, String validUserTable) throws SQLException {
//        String post = "";
        LinkedHashMap<String, String> news = new LinkedHashMap();
//        System.out.println(user);
        try (Connection connection = Connect.getConn()) {
            String sizeQuery = "SET group_concat_max_len = 100000000000000000;";

            String selectQuery = "SELECT T1.User, T1.Post_Date, T1.Text from " + textTableName
                    + " T1 "
                    + "INNER JOIN " + validUserTable + " T2 "
                    //                    + "ON T1.User = " + "\'" + "@11C1P" + "\'"
                    + "ON T1.User = T2.User "
                    + "AND Post_Date BETWEEN DATE_SUB(\"2016-11-08\", INTERVAL 45 DAY) "
                    + "AND DATE_ADD(\"2016-11-08\" ,INTERVAL 45 DAY) "
                    + "AND Text like '%trump%'"
                    + "group by T1.User, T1.Post_Date;";

//            System.out.println(selectQuery);
            Statement statement = connection.createStatement();
            statement.execute(sizeQuery);
            ResultSet result = statement.executeQuery(selectQuery);

            while (result.next()) {

                String user = result.getString(1);
                String date = result.getString(2);
                String post = result.getString(3);

                String user_date = user + " || " + date;
//                postCallback.onPostFound(user_date, post);
                news.put(user_date, post);
            }
        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
        return news;
    }

    public static void getPostsForDays(String textTableName, String from, postFound postCallback) {
        textTableName = "tbl_twitter_posts_day";

        long dataStartTime = System.currentTimeMillis();
        try (Connection connection = Connect.getConn()) {
            String sizeQuery = "SET group_concat_max_len = 100000000000000000;";

//            String selectQuery = "SELECT Post_Date, GROUP_CONCAT(Text SEPARATOR ' ') as Text from " + textTableName
//                    + " WHERE Post_Date BETWEEN DATE_SUB(\"2016-11-08\", INTERVAL 45 DAY) "
//                    + " AND DATE_ADD(\"2016-11-08\" ,INTERVAL 45 DAY) "
//                    + " group by Post_Date"
//                    + " order by 1;";
            String selectQuery = "SELECT Post_Date, Text from " + textTableName
                    + " WHERE Post_Date BETWEEN DATE_SUB(\"2016-11-08\", INTERVAL 45 DAY) "
                    + " AND DATE_ADD(\"2016-11-08\" ,INTERVAL 45 DAY) "
                    + " order by 1;";

//            System.out.println(selectQuery);
            Statement statement = connection.createStatement();
            statement.execute(sizeQuery);
            ResultSet result = statement.executeQuery(selectQuery);

            while (result.next()) {
                String date = result.getString(1);
                String post = result.getString(2);
                long dataEndTime = System.currentTimeMillis();
                System.out.println("Total Data Time: " + (dataEndTime - dataStartTime) / 1000);

                postCallback.onPostFound(date, post);
            }
        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
//        return news;
    }

    public static void createANOVATable(String tableName) {
        try (Connection connection = Connect.getConn()) {
            String dropTableQuery = "DROP TABLE IF EXISTS " + tableName;
            String createQuery = "CREATE table " + tableName + " (User varchar(100), "
                    + "Post_Date varchar(20), Adverb double, Anger double, Anx double, "
                    + "Article double, Auxverb double, Function double, I double, NegativeW double, "
                    + "Negemo double, Noun double, Posemo double, PositiveW double, Prep double, "
                    + "Pronoun double, Sad double, They double, We double);";
//            System.out.println(createQuery);
            Statement statement = connection.createStatement();
//            System.out.println(createQuery);
            statement.executeUpdate(dropTableQuery);
            statement.executeUpdate(createQuery);

        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void loadCountFileIntoTables(String filepath, String tableName) {
        try (Connection connection = Connect.getConn()) {
            String dropTableQuery = "DROP TABLE IF EXISTS " + tableName;
            String createQuery = "CREATE table " + tableName + " (User varchar(100), "
                    + "Count int, Category varchar(20), Post_Size int);";
//            System.out.println(createQuery);

//            String truncateQuery = "truncate table tbl_featVector";
            String query = "LOAD DATA LOCAL INFILE " + "\"" + filepath + "\"" + "\n"
                    + "INTO TABLE " + tableName + "\n"
                    + "COLUMNS TERMINATED BY ','\n"
                    + "LINES TERMINATED BY '\n'\n"
                    + "IGNORE 0 LINES;";
            String indexQuery = "ALTER TABLE " + tableName
                    + " ADD INDEX idx_usr(User);";

//            System.out.println(query);
            Statement statement = connection.createStatement();
//            statement.executeUpdate(truncateQuery);
            statement.executeUpdate(dropTableQuery);
            statement.executeUpdate(createQuery);
            statement.executeUpdate(query);
            statement.executeUpdate(indexQuery);

//            String trimUserFields = "update " + tableName
//                    + " set User = trim(User)";
//            String trimDateFields = "update " + tableName
//                    + " set Post_Date = trim(Post_Date)";
//
//            statement.executeUpdate(trimUserFields);
//            statement.executeUpdate(trimDateFields);
        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void loadANOVAFileIntoTables(String filepath, String tableName) {
        try (Connection connection = Connect.getConn()) {
//            String truncateQuery = "truncate table tbl_featVector";
            String query = "LOAD DATA LOCAL INFILE " + "\"" + filepath + "\"" + "\n"
                    + "INTO TABLE " + tableName + "\n"
                    + "COLUMNS TERMINATED BY ','\n"
                    + "LINES TERMINATED BY '\n'\n"
                    + "IGNORE 1 LINES;";

//            System.out.println(query);
            Statement statement = connection.createStatement();
//            statement.executeUpdate(truncateQuery);
            statement.executeUpdate(query);
            String trimUserFields = "update " + tableName
                    + " set User = trim(User)";
            String trimDateFields = "update " + tableName
                    + " set Post_Date = trim(Post_Date)";

            statement.executeUpdate(trimUserFields);
            statement.executeUpdate(trimDateFields);

        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void createEventDateTable(String tableName, String eventDate, String single_newsType) {
        try (Connection connection = Connect.getConn()) {
            String dropTableQuery = "DROP TABLE IF EXISTS " + tableName;
//            String createQuery = "create table " + tableName + " as "
//                    + "select distinct Post_Date from tbl_" + single_newsType + "_posts_user_day T1 "
//                    + "where (T1.Post_Date >=  DATE_SUB(" + "\'" + eventDate + "\'," + "INTERVAL 45 DAY) "
//                    + "AND T1.Post_Date <= DATE_ADD(" + "\'" + eventDate + "\'," + "INTERVAL 45 DAY));";

            String createQuery = "create table " + tableName + " as "
                    + "SELECT DATE_SUB(" + "\'" + eventDate + "\'," + "INTERVAL 45 DAY) + INTERVAL t.n - 1 DAY Post_Date\n"
                    + "FROM tbl_dates t\n"
                    + "WHERE t.n <= DATEDIFF(DATE_ADD(" + "\'" + eventDate + "\'," + "INTERVAL 45 DAY), "
                    + "DATE_SUB(" + "\'" + eventDate + "\'," + "INTERVAL 45 DAY)) + 1;";
//            System.out.println(createQuery);

            Statement statement = connection.createStatement();

            statement.executeUpdate(dropTableQuery);
            statement.executeUpdate(createQuery);

        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void createANOVAFVFile(String eventDate, String user, int period_size, int days, int weeks, String ANOVAFileFolderPath, List<String> categories, int totalUser) {
        try (Connection connection = Connect.getConn()) {
            String dropTableQuery = "DROP TABLE IF EXISTS tbl_single_user_FV;";

            String createQuery = "create table tbl_single_user_FV as \n"
                    + "select "
                    + "\'" + user + "\' as User,"
                    + "ifnull(T2.Post_Date, T1.Post_Date) as Post_Date,\n"
                    + "ifnull(T2.Adverb, 0) as Adverb,\n"
                    + "ifnull(T2.Anger, 0) as Anger,\n"
                    + "ifnull(T2.Anx, 0) as Anx,\n"
                    + "ifnull(T2.Article, 0) as Article,\n"
                    + "ifnull(T2.Auxverb, 0) as Auxverb,\n"
                    + "ifnull(T2.Function, 0) as Function,\n"
                    + "ifnull(T2.I, 0) as I,\n"
                    + "ifnull(T2.NegativeW, 0) as NegativeW,\n"
                    + "ifnull(T2.Negemo, 0) as Negemo,\n"
                    + "ifnull(T2.Noun, 0) as Noun,\n"
                    + "ifnull(T2.Posemo, 0) as Posemo,\n"
                    + "ifnull(T2.PositiveW, 0) as PositiveW,\n"
                    + "ifnull(T2.Prep, 0) as Prep,\n"
                    + "ifnull(T2.Pronoun, 0) as Pronoun,\n"
                    + "ifnull(T2.Sad, 0) as Sad,\n"
                    + "ifnull(T2.They, 0) as They,\n"
                    + "ifnull(T2.We, 0) as We\n"
                    + "from tbl_event_date T1\n"
                    + "LEFT JOIN tbl_user_ANOVA T2\n"
                    + "ON T1.Post_Date = T2.Post_Date\n"
                    + "AND T2.User = " + "\'" + user + "\';";

            Statement statement = connection.createStatement();

            statement.executeUpdate(dropTableQuery);
            statement.executeUpdate(createQuery);
//            System.out.println(createQuery);

            String groupProcedure = "{ call addGroupNumber(" + totalUser + ") }";
            CallableStatement cs = connection.prepareCall(groupProcedure);
            cs.execute();

            for (String category : categories) {
                if (!category.startsWith(".")) {
                    String categoryFilePath = ANOVAFileFolderPath + "/" + category + ".csv";

                    String before_date_query = "SELECT GroupNo, User, avg(" + category + ") as CategoryScore, "
                            + "\'" + "Period0" + "\' as PeriodNumber "
                            + "from tbl_single_user_FV T1 "
                            + "WHERE T1.Post_Date BETWEEN DATE_SUB((" + "\'" + eventDate + "\' - INTERVAL 1 DAY), "
                            + "INTERVAL 44 DAY) AND " + "(\'" + eventDate + "\' - INTERVAL 1 DAY);";
//                    System.out.println(before_date_query);

                    ResultSet before_date_result = statement.executeQuery(before_date_query);

                    while (before_date_result.next()) {
                        int groupNo = before_date_result.getInt(1);
                        String single_user = before_date_result.getString(2);
                        double single_category = before_date_result.getDouble(3);
                        String cycleNumber = "0";

                        String contentToWrite = groupNo + "," + single_user + "," + single_category + "," + cycleNumber;
                        IOReadWrite.writeANOVAFVIntoFile(contentToWrite, categoryFilePath);
                    }

                    String after_date_query = "SELECT GroupNo, User, avg(" + category + ") as CategoryScore, "
                            + "floor(datediff(T1.Post_Date," + "\'" + eventDate + "\'" + ") / " + days + ") AS PeriodNumber"
                            + " FROM tbl_single_user_FV T1\n"
                            + " WHERE T1.Post_Date >= " + "\'" + eventDate + "\'"
                            + " group by PeriodNumber"
                            + " limit " + period_size + ";";
//                System.out.println(after_date_query);
                    ResultSet after_date_result = statement.executeQuery(after_date_query);

                    while (after_date_result.next()) {
                        int groupNo = after_date_result.getInt(1);
                        String single_user = after_date_result.getString(2);
                        double single_category = after_date_result.getDouble(3);
                        int cycleNumber = after_date_result.getInt(4) + 1;

                        String contentToWrite = groupNo + "," + single_user + "," + single_category + "," + cycleNumber;
                        IOReadWrite.writeANOVAFVIntoFile(contentToWrite, categoryFilePath);
                    }

                    String weeks_query = "select " + "\'" + eventDate + "\'" + " + INTERVAL 14 DAY;";
                    ResultSet week_to_start = statement.executeQuery(weeks_query);
                    String days_of_week = null;

                    while (week_to_start.next()) {
                        days_of_week = week_to_start.getString(1);
                    }

                    String weeks_after_period_query = "SELECT GroupNo, User, avg(" + category + ") as CategoryScore, "
                            + "floor((datediff(T1.Post_Date," + "\'" + days_of_week + "\'" + ")) / " + 7 + ") AS PeriodNumber"
                            + " FROM tbl_single_user_FV T1\n"
                            + " WHERE T1.Post_Date >= " + "\'" + days_of_week + "\'"
                            + " group by PeriodNumber"
                            + " limit " + weeks + ";";
//                System.out.println(query);
                    ResultSet weeks_after_period_result = statement.executeQuery(weeks_after_period_query);

                    while (weeks_after_period_result.next()) {
                        int groupNo = weeks_after_period_result.getInt(1);
                        String single_user = weeks_after_period_result.getString(2);
                        double single_category = weeks_after_period_result.getDouble(3);
                        int cycleNumber = weeks_after_period_result.getInt(4) + 8;

                        String contentToWrite = groupNo + "," + single_user + "," + single_category + "," + cycleNumber;
                        IOReadWrite.writeANOVAFVIntoFile(contentToWrite, categoryFilePath);
                    }
                }
            }

        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException | IOException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static List<String> getCategories() {
        List<String> categories = new ArrayList();

        try (Connection connection = Connect.getConn()) {

            String selectQuery = "SELECT * from tbl_category;";

            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(selectQuery);

            while (result.next()) {
                String category = result.getString(1);
                categories.add(category);
            }
        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
        return categories;
    }

    public static void createValidUserTable(String validUserTable, String userPostTable) {
        try (Connection connection = Connect.getConn()) {
            String dropTableQuery = "DROP TABLE IF EXISTS " + validUserTable;
            String createQuery = "CREATE table " + validUserTable
                    + " SELECT distinct User from " + userPostTable
                    //                    + " WHERE Post_Date BETWEEN DATE_SUB(\"2016-11-08\", INTERVAL 45 DAY) "
                    //                    + " AND DATE_ADD(\"2016-11-08\" ,INTERVAL 45 DAY)\n"
                    + " WHERE Text like '%trump%'";
//                    + " group by User;\n";
            // + " having count(*) > 50;";
            String indexQuery = "ALTER table " + validUserTable
                    + " add index idx_usr(User)";
//            System.out.println(createQuery);
            Statement statement = connection.createStatement();
//            System.out.println(createQuery);
            statement.executeUpdate(dropTableQuery);
            statement.executeUpdate(createQuery);
            statement.executeUpdate(indexQuery);
        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void createValidTrumpUser(String validUserTable, String countTable) {
        try (Connection connection = Connect.getConn()) {
            String dropTableQuery = "DROP TABLE IF EXISTS " + validUserTable;

            String createQuery = "CREATE TABLE " + validUserTable
                    + "(Id int auto_increment not null primary key, User varchar(50));";

            String insertQuery = "INSERT INTO " + validUserTable
                    + " select 0, T1.User from \n"
                    + "(select User, Count, Post_Size from " + countTable
                    + " having Count/Post_Size >= Round(0.0000,6) and Count >= 5) T1;";

            String indexQuery = "ALTER table " + validUserTable
                    + " add index idx_usr(User)";

            Statement statement = connection.createStatement();
            System.out.println(insertQuery);

            statement.executeUpdate(dropTableQuery);

            statement.executeUpdate(createQuery);
            statement.executeUpdate(insertQuery);
            statement.executeUpdate(indexQuery);
        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void createNonTrumpUser(String postTable, String validUserTable, String countTable) {
try (Connection connection = Connect.getConn()) {

            String insertQuery = "INSERT INTO " + validUserTable +
                    " SELECT 0, T1.user from " + postTable + " T1 "
                    + "LEFT JOIN " + countTable + " T2 "
                    + "ON T1.User = T2.User\n"
                    + "WHERE T2.User IS NULL\n"
                    + "ORDER BY RAND()\n"
                    + "limit 500";
//    System.out.println(insertQuery);
//            String indexQuery = "ALTER table " + validUserTable
//                    + " add index idx_usr(User)";

            Statement statement = connection.createStatement();
//            System.out.println(createQuery);

            statement.executeUpdate(insertQuery);
//            statement.executeUpdate(indexQuery);
        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void createUserPostTable(String userPostTableName) {
        try (Connection connection = Connect.getConn()) {

            String selectQuery = "create table " + userPostTableName
                    + " select User, GROUP_CONCAT(Text, \" \") as Text\n"
                    + " from tbl_twitter_posts_user_day \n"
                    + " WHERE Post_Date BETWEEN DATE_SUB(\"2016-11-08\", INTERVAL 45 DAY) "
                    + " AND DATE_ADD(\"2016-11-08\" ,INTERVAL 45 DAY)\n"
                    + " AND (User not like '%news%'\n"
                    + " AND User not like '%cnn%')\n"
                    + " group by User\n"
                    + " having count(*) > 50;";
            String indexQuery = "ALTER table " + userPostTableName
                    + "add index idx_usr(User)";

            Statement statement = connection.createStatement();
            statement.executeQuery(selectQuery);
            statement.executeUpdate(indexQuery);

        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void createRankTable(String rankTable, String FV_table) {
        try (Connection connection = Connect.getConn()) {
            String dropTable = "DROP TABLE IF EXISTS " + rankTable;

            String selectQuery = "create table " + rankTable
                    + "(ID int primary key auto_increment not null, "
                    + "Term_Freq double, User varchar(100));";
            String insertQuery = "insert into " + rankTable + " (ID, Term_Freq, User)\n"
                    + "select \"0\", Avg(Count/Post_Size) as Term_Freq, SPLIT_STR(User, '||', 1) as User\n"
                    + "from " + FV_table
                    + " group by SPLIT_STR(User, '||', 1)\n"
                    + "order by 2 desc";
//            System.out.println(insertQuery);

            Statement statement = connection.createStatement();

            statement.executeUpdate(dropTable);
            statement.executeUpdate(selectQuery);
            statement.executeUpdate(insertQuery);

        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void createDateTable(String dateTable) {
        try (Connection connection = Connect.getConn()) {
            String dropTable = "DROP TABLE IF EXISTS " + dateTable;

            String selectQuery = "create table " + dateTable
                    + " (n int not null primary key);";
            String insertQuery = "INSERT INTO " + dateTable
                    + " SELECT a.N + b.N * 10 + c.N * 100 + 1 n\n"
                    + "  FROM \n"
                    + " (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a\n"
                    + ",(SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b\n"
                    + ",(SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c\n"
                    + "ORDER BY n;";
//            System.out.println(insertQuery);

            Statement statement = connection.createStatement();

            statement.executeUpdate(dropTable);
            statement.executeUpdate(selectQuery);
            statement.executeUpdate(insertQuery);

        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
