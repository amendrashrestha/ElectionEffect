/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utilities;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author amendrashrestha
 */
public class Test {

    private static final Pattern EMOTION_PATTERN = Pattern.compile("[\\//()]");

    public static void main(String args[]) {

        String searchWord = "armé*";
        String post = "arméndra";
        Pattern p;
        int count = 0;

        Matcher emo_match = EMOTION_PATTERN.matcher(searchWord);

//            if (searchWord.endsWith("*") && !searchWord.startsWith("*")) {
////                System.out.println(searchWord);
//                String new_text = Pattern.quote(searchWord.replace("*", ""));
////                p = Pattern.compile("\\b" + new_text + "\\w*\\p{L}*");
//                p = Pattern.compile("\\b" + new_text + "\\w*");
//
//            } else 
        if (searchWord.contains("*")) {
//                System.out.println(searchWord);
            String new_text = searchWord.replace("*", "");
//                String sPattern = "(?i)\\w*" + Pattern.quote(new_text) + "\\w*\\p{L}*";
            String sPattern = "(?i)\\w*" + Pattern.quote(new_text) + "\\w*";
//                String sPattern = Pattern.quote(new_text);
            p = Pattern.compile(sPattern);

        } else if (emo_match.find()) {
//                    System.out.println(searchWord);
            searchWord = Pattern.quote(searchWord);
            p = Pattern.compile(searchWord);
//                System.out.println("Else if: " + p);
        } else {
            p = Pattern.compile("\\b" + Pattern.quote(searchWord) + "\\s");
//                System.out.println("Else: " + p);
        }

        Matcher m = p.matcher(post);

        while (m.find()) {
//                try {
//                    System.out.println();
            System.out.println("Found: " + m.group());
//                    writeWordsIntoFile(m.group(), liwcCountFile);
            count += 1;
//                System.out.print(".");
        }
//                catch (IOException ex) {
//                    Logger.getLogger(IOReadWrite.class.getName()).log(Level.SEVERE, null, ex);
//                }
//            }
//            System.out.println("************");
    }

}
