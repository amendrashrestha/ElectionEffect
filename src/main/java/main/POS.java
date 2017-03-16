package main;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import java.io.IOException;
import utilities.IOProperties;

/**
 *
 * @author amendrashrestha
 */
public class POS {

    public static void main(String args[]) throws IOException, ClassNotFoundException {
        String text = "My name is Amendra Shrestha. This is big test. Donald Trump would not beautifully americans president...";
        MaxentTagger tagger = new MaxentTagger(IOProperties.TAGGER_MODEL);

        int nouns = 0;
        int auxillary_verb = 0;
        int adverbs = 0;
        // The tagged string
        final String[] tokens = tagger.tagString(text).split(" ");

        for (String single_token : tokens) {
            System.out.println(single_token);
            final int lastUnderscoreIndex = single_token.lastIndexOf("_");
            final String realToken = single_token.substring(lastUnderscoreIndex + 1);
            if (realToken.matches("(?i)NN.*")) 
//            if ("NN".equals(realToken) || "NNS".equals(realToken) || "NNP".equals(realToken) || "NNPS".equals(realToken))
                nouns++;            
        }

        System.out.println(String.format("Nouns: %d", nouns));

    }

}
