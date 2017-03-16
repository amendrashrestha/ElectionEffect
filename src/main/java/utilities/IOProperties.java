package utilities;

/**
 *
 * @author amendrashrestha
 */
public class IOProperties {
    
    public static final String TAGGER_MODEL = System.getProperty("user.dir") + "/english-left3words-distsim.tagger";
//    public static final String TAGGER_MODEL = System.getProperty("user.home") + "/Downloads/Amendra/Tagger/english-left3words-distsim.tagger";

    public static final String LIWC_COUNT_FILEPATH = System.getProperty("user.home") + "/Desktop/Trump/LIWC_Count_File";
    public static final String LIWC_LIMITED_CATEGORY_WORD_FILEPATH = System.getProperty("user.home") + "/Desktop/Trump/LIWC/";
    
    public static final String USER_PERIOD_COUNT_FILEPATH = System.getProperty("user.home") + "/Desktop/IslamicForum/Period_Count/";
 
    public static final String FEATURE_VECTOR_FILEPATH = System.getProperty("user.home") + "/Desktop/Trump/Feature_Vector/";
    public static final String ANOVA_FEATURE_VECTOR_FILEPATH = System.getProperty("user.home") + "/Desktop/Trump/ANOVA_Files/";
    

}
