package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Main
{
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static void main(String[] args) throws Exception
    {
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");

        FileSystem hdfs = FileSystem.get(
                new URI("hdfs://36c8659fa953:8020"), configuration
        );
        Path file = new Path("hdfs://36c8659fa953:8020/test/file2.txt");

        if (hdfs.exists(file)) {
            hdfs.delete(file, true);
        }

        OutputStream os = hdfs.create(file);
        BufferedWriter br = new BufferedWriter(
                new OutputStreamWriter(os, "UTF-8")
        );

//        for(int i = 0; i < 100000; i++) {
//            br.write(getRandomWord() + " ");
//        }
        br.write(getRandomWord());

        br.flush();
        br.close();
        hdfs.close();
    }

    private static String getRandomWord() throws IOException {
        String sourceFile = "res/file2.txt";
        List<String> text = Files.readAllLines(Paths.get(sourceFile));
        StringBuilder builder = new StringBuilder();
        for(String string: text){
            builder.append(string);
        }
//        int length = 2 + (int) Math.round(10 * Math.random());
//        int symbolsCount = symbols.length();
//        for(int i = 0; i < length; i++) {
//            builder.append(symbols.charAt((int) (symbolsCount * Math.random())));
//        }
        return builder.toString();
    }
}