package utilities;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import model.Database;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author amendrashrestha
 */
public class IOReadWrite {
//    private static final Pattern UNDESIRABLES = Pattern.compile("[-−—–*,.;!?<>%]"); //Most Important Feature Count

    private static final Pattern UNDESIRABLES = Pattern.compile("[\"*,.;!?<>%…-]");
    private static final MaxentTagger tagger = new MaxentTagger(IOProperties.TAGGER_MODEL);
    static AtomicBoolean taskComp = new AtomicBoolean(false);

    public static void returnWordCount(String dataType, String LIWCCountFolderPath, File liwcCountFile) throws IOException, ClassNotFoundException,
            InstantiationException, IllegalAccessException {

        ArrayList<String> fileList = getListOfFile(LIWCCountFolderPath);
        for (String filepath : fileList) {
            if (!filepath.equals(".DS_Store")) {
                ArrayList<String> keywordList = IOReadWrite.loadLiwcWord(LIWCCountFolderPath, filepath);
                searchWord(keywordList, filepath, dataType, liwcCountFile);
            }
        }
    }

    static void searchWord(ArrayList<String> searchTermsList, String filename, String dataType, File liwcCountFile)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        String tableName = "tbl_news_" + dataType;
//        String tableName = "tbl_" + dataType;

        List<String> news = Database.getPosts(tableName);
        int postID = 1;
        for (String single_news : news) {
            String filterPost = filterPost(single_news);
            int postSize = extractWords(filterPost).size();

            liwcWordCount(searchTermsList, filterPost, filename, postSize, dataType, String.valueOf(postID), liwcCountFile, null);
            postID++;
        }
    }

    static void liwcWordCount(List<String> liwcList, String post, String category, int postSize, String filename,
            String user, File liwcCountFile, File LIWCwordFoundFile) {
        Pattern p;
        int count = 0;
        post = String.format("%-1s", post.toLowerCase());

        for (String searchWord : liwcList) {
            if (searchWord.contains("*")) {
                String new_text = searchWord.replace("*", "");
                String sPattern = "(?i)\\w*" + Pattern.quote(new_text) + "\\w*";

                p = Pattern.compile(sPattern);
            } else {
                p = Pattern.compile("\\b" + Pattern.quote(searchWord) + "\\s");
            }
            Matcher m = p.matcher(post);

            while (m.find()) {
                count += 1;
            }
        }
        String contentToWrite = (user + "," + count + "," + category + "," + postSize);
        IOReadWrite.writeIntoFile(contentToWrite, filename, category, liwcCountFile);
    }

    /**
     * load LIWC words into global hash map converting them in lower case
     *
     * @param filePath
     * @param categoryFileName
     * @return
     * @throws FileNotFoundException
     */
    public static ArrayList<String> loadLiwcWord(String filePath, String categoryFileName
    ) throws FileNotFoundException {
        ArrayList<String> wordList = new ArrayList();
        Scanner scanner = new Scanner(new FileReader(filePath + "/" + categoryFileName));

        while (scanner.hasNextLine()) {
            String[] columns = scanner.nextLine().split(",");
            wordList.add(columns[0].toLowerCase().trim());
        }
        return wordList;
    }

    public static ArrayList<String> getListOfFile(String fileListPath) throws IOException {
        ArrayList<String> filepath = new ArrayList();

        Files.walk(Paths.get(fileListPath)).forEach(filePath -> {
            if (Files.isRegularFile(filePath)) {
                filepath.add(filePath.getFileName().toString());
            }
        });
        return filepath;
    }

    public static List<String> getAllDirectories(String basePath) {
        File file = new File(basePath);
        String[] directories = file.list((File dir, String name) -> new File(dir, name).isDirectory());
        return Arrays.asList(directories);
    }

    /**
     * return list of words from the post
     *
     * @param text
     * @return
     */
    public static List<String> extractWords(String text) {
        text = text.toLowerCase().trim();
        List<String> wordList = new ArrayList<>();
        String[] words = text.split("\\s+");

        wordList.addAll(Arrays.asList(words));
        return wordList;
    }

    public static String filterPost(String post) {
        post = removeUrl(post);
        post = UNDESIRABLES.matcher(post).replaceAll(" ") + " ";
        post = replaceUsername(post);
        return post;
    }

    public static String removeUrl(String text) {
        text = text.replaceAll("(https?|ftp|file|http)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]", "");
        return text;
    }

    public static String replaceUsername(String post) {
        String patternStr = "(?:\\s|\\A)[@]+([A-Za-z0-9-_]+)";
        Pattern pattern = Pattern.compile(patternStr);
        Matcher matcher = pattern.matcher(post);

        while (matcher.find()) {
            String result = matcher.group();
//            System.out.println(result);
            post = post.replace(result, "");
        }
        return post;
    }

    public static void writeIntoFile(String content, String foldername, String filename, File liwcCountFile) {
        try {
//        System.out.println(liwcCountFile);
            String folderPath = liwcCountFile + "/" + filename;
            liwcCountFile.mkdir();
            File dir = new File(folderPath);
            dir.createNewFile();

            try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(folderPath, true)))) {
                out.println(content);
            } catch (IOException e) {
            }
        } catch (IOException ex) {
            Logger.getLogger(IOReadWrite.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Create features for each user and writes in files
     *
     * @param resultList
     * @param filename
     * @throws IOException
     */
    public static void ExtractDateandCount(List<String> resultList, String filename) throws IOException {
        ArrayList<String> featCount = new ArrayList();

        resultList.stream().map((count) -> count.split(",")).forEach((result) -> {
//            String date = result[0];
            String date = result[0].replace("||", ", ");
            String keywordCount = result[1];

            if (featCount.contains(date)) {
                featCount.add(keywordCount);
            } else {
                featCount.add(date);
                featCount.add(keywordCount);
            }
        });
//        featCount.add(dataClass);
        writeIntoFile(featCount, filename);
    }

    public static < T> void writeIntoFile(Collection<T> featCount, String filename) throws IOException {

//        String folderPath = IOProperties.FEATURE_VECTOR_FILEPATH + "FeatureVector.csv";
        try (FileWriter fw = new FileWriter(filename, true) //the true will append the new data
                ) {
            fw.write(featCount.toString().replace("[", "").replace("]", "").replaceAll(",", ",") + "\n");//appends the string to the file
        }
    }

    public static void writeANOVAFVIntoFile(String featCount, String filename) throws IOException {
        try (FileWriter fw = new FileWriter(filename, true) //the true will append the new data
                ) {
            fw.write(featCount.replace("[", "").replace("]", "").replaceAll(",", ",") + "\n");//appends the string to the file
        }
    }

    public static <Double> void writeForumFVFile(ArrayList<Double> featCount, File filename) throws IOException {

//        String folderPath = IOProperties.FEATURE_VECTOR_FILEPATH + "FeatureVector.csv";
        try (FileWriter fw = new FileWriter(filename, true) //the true will append the new data
                ) {
            fw.write(featCount.toString().replace("[", "").replace("]", "").replaceAll(",", ",") + "\n");//appends the string to the file
        }
    }

    public static void writeWordsIntoFile(String word, File filename) throws IOException {

//        String folderPath = IOProperties.FEATURE_VECTOR_FILEPATH + "FeatureVector.csv";
        try (FileWriter fw = new FileWriter(filename, true) //the true will append the new data
                ) {
            fw.write(word + "\n");//appends the string to the file
        }
    }

    public static void createFVofForum(String forum, File forumName, String wordCountFilesPath) {
        try {

            if (!forumName.exists()) {
                IOReadWrite.createFileWithHeader(forumName, wordCountFilesPath);
            }
            ArrayList<Double> count = Database.getFVOfForum(forum);
            writeForumFVFile(count, forumName);

        } catch (IOException ex) {
            Logger.getLogger(IOReadWrite.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void createFileWithHeader(File fileName, String LIWC_category_filepath) throws IOException {
        List<String> headers = getListOfFile(LIWC_category_filepath);
        List<String> headerList = new ArrayList();

        for (int i = 0; i < headers.size(); i++) {
            if (headers.get(i).startsWith(".")) {
                headers.remove(i);
            }
            if (headers.get(i).contains(".txt")) {
                headerList.add(headers.get(i).replace(".txt", ""));
            } else {
                headerList.add(headers.get(i));
            }
        }

        headerList.add(0, "Date");
        headerList.add(1, "User");
        // adding time field in the file
//        headerList.add(headers.size(), "Time");
//        headers.add(headers.size(), "Class");
        String header = String.join(", ", headerList);

        if (!fileName.exists()) {
            fileName.createNewFile();
        }
        FileWriter fw = new FileWriter(fileName.getAbsoluteFile(), true);
        try (BufferedWriter bw = new BufferedWriter(fw)) {
            bw.write(header + "\n");
        }
    }

    public static void createANOVAFileWithHeader(File fileName) throws IOException {
        List<String> headerList = new ArrayList();

        headerList.add(0, "GroupNo");
        headerList.add(1, "User");
        headerList.add(2, "CategoryScore");
        headerList.add(3, "PeriodNumber");
        // adding time field in the file
//        headerList.add(headers.size(), "Time");
//        headers.add(headers.size(), "Class");
        String header = String.join(", ", headerList);

        if (!fileName.exists()) {
            fileName.createNewFile();
        }
        FileWriter fw = new FileWriter(fileName.getAbsoluteFile(), true);
        try (BufferedWriter bw = new BufferedWriter(fw)) {
            bw.write(header + "\n");
        }
    }

    /**
     * Write the list of file with username
     *
     * @param toWrite
     * @param filePath
     * @param fileName
     * @throws IOException
     */
    public static void writeInToFile(String toWrite, String filePath, String fileName) throws IOException {
        fileName = filePath + fileName;
        try (FileWriter fw = new FileWriter(fileName, true)) {
            fw.write(toWrite + "\n");
        }
    }

    public static void categoryCount(String LIWCCountFolderPath, File liwcCountFile, String postTableName, String validUserTable) {
        try {
            ArrayList<String> fileList = getListOfFile(LIWCCountFolderPath);

            for (String filepath : fileList) {
                if (!filepath.startsWith(".")) {
                    ArrayList<String> keywordList = IOReadWrite.loadLiwcWord(LIWCCountFolderPath, filepath);
                    searchCategoryForValidUser(postTableName, keywordList, filepath, liwcCountFile, validUserTable);
//                countLIWCForUser(keywordList, filepath, liwcCountFile);
//                    searchCategoryWithDay(single_newsType, keywordList, filepath, liwcCountFile);
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(IOReadWrite.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    static void searchCategoryWordForUserWithPeriod(ArrayList<String> searchTermsList, String filename, File liwcCountFile)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
        String textTableName = "tbl_vnnforum_merge_day";

        List<String> users = Database.getUsers(textTableName);

        for (String user : users) {
            List<String> posts = Database.returnValidUserPost(user);
            if (posts.size() > 0) {
                int i = 1;
                for (String single_post : posts) {
                    String username = user + "_" + Integer.toString(i);
                    if (single_post.length() != 0) {
                        String filterPost = filterPost(single_post);
                        int postSize = extractWords(filterPost).size();
                        liwcWordCount(searchTermsList, filterPost, filename, postSize, textTableName, username, liwcCountFile, null);
                    }
                    i++;
                }
            }
        }
    }

    static void searchCategoryWordForUser(ArrayList<String> searchTermsList, String filename, File liwcCountFile)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
        String textTableName = "tbl_vnnforum_merge_user_day";

        List<String> users = Database.getUsers(textTableName);
        List<String> validUsers = new ArrayList();

        for (String user : users) {
            int isValidUser = Database.returnValidUser(user);
            if (isValidUser == 1) {
                validUsers.add(user);
            }
        }

        for (String user : validUsers) {
            int i = 1;
            List<String> posts = Database.getAllPostsForUser(textTableName, user);

            for (String single_post : posts) {
                String temp_user = Integer.toString(i) + "_" + user;
                if (single_post.length() != 0) {
                    String filterPost = filterPost(single_post);
                    int postSize = extractWords(filterPost).size();
                    liwcWordCount(searchTermsList, filterPost, filename, postSize, textTableName, temp_user, liwcCountFile, null);
                }
                i++;
            }
        }
    }

    static void countLIWCForUser(ArrayList<String> searchTermsList, String filename, File liwcCountFile)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
        String userPostTableName = "tbl_twitter_posts_user";
//        String LIWCwordFound = IOProperties.FEATURE_VECTOR_FILEPATH + "LIWCWordFound.tsv";
//        File LIWCwordFoundFile = new File(LIWCwordFound);
        Database.createUserPostTable(userPostTableName);

        List<String> users = Database.getTrumpUsers(userPostTableName);

        for (String user : users) {
            List<String> posts = Database.getAllPostsForUser(userPostTableName, user);

            for (String single_post : posts) {
                if (single_post.length() != 0) {
                    String filterPost = filterPost(single_post);
                    int postSize = extractWords(filterPost).size();
                    liwcWordCount(searchTermsList, filterPost, filename, postSize, userPostTableName, user, liwcCountFile, null);
                }
            }
        }
    }

    public static void countWithYear(String LimitedLIWCFeatureFile, File liwcCountFile) {
        try {
            ArrayList<String> fileList = getListOfFile(LimitedLIWCFeatureFile);
            for (String filepath : fileList) {
                if (!filepath.equals(".DS_Store")) {
                    ArrayList<String> keywordList = IOReadWrite.loadLiwcWord(LimitedLIWCFeatureFile, filepath);
                    searchCategoryWordWithYear(keywordList, filepath, liwcCountFile);
                }
            }
        } catch (IOException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(IOReadWrite.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    static void searchCategoryWordWithYear(ArrayList<String> searchTermsList, String filename, File liwcCountFile)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        String tableName = "tbl_count_year";
        String[] years = new String[]{"2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015"};
        String textTableName = "tbl_flashback_year";

        for (String single_year : years) {
            try {
                List<String> news = Database.getPostsForYear(textTableName, single_year);
                for (String single_new : news) {
                    if (single_new.length() != 0) {
                        String filterPost = filterPost(single_new);
                        int postSize = extractWords(filterPost).size();
                        liwcWordCount(searchTermsList, filterPost, filename, postSize, tableName, single_year, liwcCountFile, null);
                    }
                }
            } catch (SQLException ex) {
                Logger.getLogger(IOReadWrite.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static void countUserWithYear(String LIWCCountFolderPath, File liwcCountFile) throws IOException {
        ArrayList<String> fileList = getListOfFile(LIWCCountFolderPath);
        for (String filepath : fileList) {
            if (!filepath.equals(".DS_Store")) {
                try {
                    ArrayList<String> keywordList = IOReadWrite.loadLiwcWord(LIWCCountFolderPath, filepath);
//                    searchCategoryWordUser(keywordList, filepath, liwcCountFile);
                    searchCategoryWithDay(null, keywordList, filepath, liwcCountFile);
                } catch (FileNotFoundException ex) {
                    Logger.getLogger(IOReadWrite.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    static void searchCategoryWithDay(String newstype, ArrayList<String> searchTermsList, String filename, File liwcCountFile) {
        taskComp.set(false);
        String postTableName = "tbl_" + newstype + "_posts_user_day";
        String validUserTable = "tbl_" + newstype + "_valid_user";

        String from = "2016-11-08"; //  Election day

        ExecutorService pool = Executors.newFixedThreadPool(8);
        ExecutorService pool1 = Executors.newSingleThreadExecutor();
        System.out.println("Category: " + filename);

        Database.getPostsForDays(postTableName, from, validUserTable, (String user, String post) -> {
            String filterPost = filterPost(post);
//            int postSize = extractWords(filterPost).size();
//            System.out.println(filterPost);
            int postSize = filterPost.split("\\s+").length;

            CountLIWCTask task = new CountLIWCTask(user, filterPost, searchTermsList);

            Future<Integer> result = pool.submit(task);
            pool1.submit(new LIWCResultHandler(result, task, filename, postSize, postTableName, liwcCountFile));

//            liwcWordCount(searchTermsList, filterPost, filename, postSize, postTableName, user, liwcCountFile, null);
        });
        Future<Integer> result = pool.submit(new CountLIWCTask(null, null, null));
        pool1.submit(new LIWCResultHandler(result, null, null, 0, null, null));

        while (taskComp.get() == false) {
            try {
                Thread.sleep(2000);
                System.out.println("Waiting for program to complete....");
            } catch (InterruptedException ex) {
                Logger.getLogger(IOReadWrite.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        System.out.println("Program Completed....");
        pool.shutdown();
        pool1.shutdown();
        System.out.println("System shutting down....");
    }

    static void searchCategoryWordUser(ArrayList<String> searchTermsList, String filename, File liwcCountFile)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        String userTableName = "tbl_user_07_to_15";
        String textTableName = "tbl_flashback_day";

        String[] years = new String[]{"2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015"};
        List<String> users = Database.getUsers(userTableName);

        for (String user : users) {
//            System.out.println(user);
//                int i = 1;
            for (String single_year : years) {
                try {
                    String news = Database.getPostsForUser(textTableName, user, single_year);

//                        String new_user = user + i;
                    if (news.length() != 0) {
                        String filterPost = filterPost(news);
                        int postSize = extractWords(filterPost).size();
                        liwcWordCount(searchTermsList, filterPost, filename, postSize, userTableName, single_year, liwcCountFile, null);
//                            i++;
                    }

                } catch (SQLException ex) {
                    Logger.getLogger(IOReadWrite.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public static void countNoun(File liwcCountFile, String postTableName, String validUserTable) {
        taskComp.set(false);

        ExecutorService pool = Executors.newFixedThreadPool(8);
        ExecutorService pool1 = Executors.newSingleThreadExecutor();

//        String postTableName = "tbl_" + newsType + "_posts_user_day";
//        String validUserTable = "tbl_" + newsType + "_valid_user";
//        String date_to_check = "2016-11-08";
        String noun_count_file = "Noun";

//        Database.getPostsForDays(postTableName, date_to_check, validUserTable, (String user, String post) -> {
        Database.getPostsForUser(postTableName, validUserTable, (String user, String post) -> {
            String filterPost = filterPost(post);
//            int postSize = extractWords(filterPost).size();
            int postSize = filterPost.split("\\s+").length;

            CountNounTask task = new CountNounTask(user, filterPost);

            Future<Integer> result = pool.submit(task);
            pool1.submit(new nounResultHandler(result, task, noun_count_file, postSize, postTableName, liwcCountFile));
        });

        Future<Integer> result = pool.submit(new CountNounTask(null, null));
        pool1.submit(new nounResultHandler(result, null, null, 0, null, null));

        while (taskComp.get() == false) {
            try {
                Thread.sleep(2000);
                System.out.println("Waiting for program to complete....");
            } catch (InterruptedException ex) {
                Logger.getLogger(IOReadWrite.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        System.out.println("Program Completed....");
        pool.shutdown();
        pool1.shutdown();
        System.out.println("System shutting down....");
//        Future<Integer> result = pool.submit(task);
    }

    private static final class CountNounTask implements Callable<Integer> {

        String user;
        String post;

        public CountNounTask(String user, String post) {
            this.user = user;
            this.post = post;
        }

        @Override
        public Integer call() {
            if (user == null) {
                return -100;
            }

            int nouns = 0;

//            long startTime = System.currentTimeMillis();
            final String[] tokens = tagger.tagString(post).split(" ");

            for (String single_token : tokens) {
                final int lastUnderscoreIndex = single_token.lastIndexOf("_");
                final String realToken = single_token.substring(lastUnderscoreIndex + 1);
                if (realToken.matches("(?i)NN.*")) {
                    nouns++;
                }
            }

//            long endTime = System.currentTimeMillis();
//            System.out.println("Time Taken: " + (endTime - startTime)/1000);
            return nouns;
        }
    }

    private static final class nounResultHandler implements Runnable {

        Future<Integer> result;
        CountNounTask task;
        String noun_count_file;
        int postSize;
        String postTableName;
        File liwcCountFile;

        public nounResultHandler(Future<Integer> result, CountNounTask task, String noun_count_file, int postSize,
                String postTableName, File liwcCountFile) {
            this.result = result;
            this.task = task;
            this.noun_count_file = noun_count_file;
            this.postSize = postSize;
            this.postTableName = postTableName;
            this.liwcCountFile = liwcCountFile;
        }

        @Override
        public void run() {

            try {

                if (result.get() == -100) {
                    taskComp.set(true);
                }

                try {
                    String contentToWrite = (task.user + "," + result.get() + "," + noun_count_file + "," + postSize);
                    IOReadWrite.writeIntoFile(contentToWrite, postTableName, noun_count_file, liwcCountFile);
//                    System.out.println(contentToWrite);
                } catch (InterruptedException | ExecutionException ex) {
                    Logger.getLogger(IOReadWrite.class.getName()).log(Level.SEVERE, null, ex);
                }
            } catch (InterruptedException | ExecutionException ex) {
                Logger.getLogger(IOReadWrite.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    static void searchCategoryForValidUser(String postTableName, ArrayList<String> searchTermsList, String categoryName, File liwcCountFile, String validUserTable) {
        taskComp.set(false);
//        String postTableName = "tbl_" + single_newsType + "_posts_user";

        ExecutorService pool = Executors.newFixedThreadPool(8);
        ExecutorService pool1 = Executors.newSingleThreadExecutor();

        Database.getPostsForUser(postTableName, validUserTable, (String user, String post) -> {
            String filterPost = filterPost(post);
            int postSize = filterPost.split("\\s+").length;

            CountLIWCTask task = new CountLIWCTask(user, filterPost, searchTermsList);

            Future<Integer> result = pool.submit(task);
            pool1.submit(new LIWCResultHandler(result, task, categoryName, postSize, postTableName, liwcCountFile));
        });

        Future<Integer> result = pool.submit(new CountLIWCTask(null, null, null));
        pool1.submit(new LIWCResultHandler(result, null, null, 0, null, null));

        while (taskComp.get() == false) {
            try {
                Thread.sleep(2000);
                System.out.println("Waiting for program to complete....");
            } catch (InterruptedException ex) {
                Logger.getLogger(IOReadWrite.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        System.out.println("Program Completed....");
        pool.shutdown();
        pool1.shutdown();
        System.out.println("System shutting down....");
    }

    private static class CountLIWCTask implements Callable<Integer> {

        String user;
        String post;
        List<String> liwcList;

        public CountLIWCTask(String user, String post, List<String> liwcList) {
            this.user = user;
            this.post = post;
            this.liwcList = liwcList;
        }

        @Override
        public Integer call() {
            if (user == null) {
                return -100;
            }
//            System.out.println(liwcList);

//            long startTime = System.currentTimeMillis();
            Pattern p;
            int count = 0;
            post = String.format("%-1s", post.toLowerCase());

            for (String searchWord : liwcList) {
                if (searchWord.contains("*")) {
                    String new_text = searchWord.replace("*", "");
                    String sPattern = "(?i)\\w*" + Pattern.quote(new_text) + "\\w*";

                    p = Pattern.compile(sPattern);
                } else {
                    p = Pattern.compile("\\b" + Pattern.quote(searchWord) + "\\s");
                }
                Matcher m = p.matcher(post);

                while (m.find()) {
                    count += 1;
                }
            }
//            long endTime = System.currentTimeMillis();
//            System.out.println("Time Taken: " + (endTime - startTime) / 1000);
            return count;
        }
    }

    private static final class LIWCResultHandler implements Runnable {

        Future<Integer> result;
        CountLIWCTask task;
        String noun_count_file;
        int postSize;
        String postTableName;
        File liwcCountFile;

        public LIWCResultHandler(Future<Integer> result, CountLIWCTask task, String noun_count_file, int postSize,
                String postTableName, File liwcCountFile) {
            this.result = result;
            this.task = task;
            this.noun_count_file = noun_count_file;
            this.postSize = postSize;
            this.postTableName = postTableName;
            this.liwcCountFile = liwcCountFile;
        }

        @Override
        public void run() {

            try {
                if (result.get() == -100) {
                    taskComp.set(true);
                }

                try {
                    String contentToWrite = (task.user + "," + result.get() + "," + noun_count_file + "," + postSize);
                    IOReadWrite.writeIntoFile(contentToWrite, postTableName, noun_count_file, liwcCountFile);
//                System.out.println(contentToWrite);
                } catch (InterruptedException | ExecutionException ex) {
                    Logger.getLogger(IOReadWrite.class.getName()).log(Level.SEVERE, null, ex);
                }
            } catch (InterruptedException | ExecutionException ex) {
                Logger.getLogger(IOReadWrite.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    //creates ANOVA files
    public static void createANOVAForumDirectory(String folderPath) {
        File dir = new File(folderPath);
        if (!dir.exists()) {
            dir.mkdir();
        }
    }

}
