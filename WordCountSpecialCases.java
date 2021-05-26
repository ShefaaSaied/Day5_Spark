package com.sparkTutorial.rdd;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.Map;

public class WordCountSpecialCases {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/word_count.text");

        // Splitting with multiple delimiters using the OR regex expression |
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" |\\,|\\.|\\:|\\;|\\)|\\(|\\–|\\_")).iterator());
        // Mapping all words to lower or upper case to count them
        JavaRDD<String> lowerCaseWords = words.map(line -> line.toLowerCase());

        Map<String, Long> wordCounts = lowerCaseWords.countByValue();

        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }
}