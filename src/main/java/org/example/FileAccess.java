package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class FileAccess
{
    private String rootPath;
    private FileSystem hdfs;
    /**
     * Initializes the class, using rootPath as "/" directory
     *
     * @param rootPath - the path to the root of HDFS,
     * for example, hdfs://localhost:32771
     */
    public FileAccess(String rootPath) throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");
        this.hdfs = FileSystem.get(new URI("hdfs://5e5f0a7c3ac1:8020"), configuration);
        this.rootPath = rootPath + '/';
    }

    /**
     * Creates empty file or directory
     *
     * @param path
     */
    public void create(String path) throws IOException {
        Path file = new Path(rootPath + path);
        if(path.charAt(path.length()) == '/') hdfs.mkdirs(file);
        else hdfs.create(file);
    }

    /**
     * Appends content to the file
     *
     * @param path
     * @param content
     */
    public void append(String path, String content) throws IOException {
        Path file = new Path(rootPath + path);
        OutputStream os = hdfs.append(file);
        BufferedWriter br = new BufferedWriter(
                new OutputStreamWriter(os, "UTF-8")
        );
        br.write(content);
        br.flush();
        br.close();
    }

        /**
         * Returns content of the file
         *
         * @param path
         * @return
         */
    public String read(String path) throws IOException {
        Path file = new Path(rootPath + path);
        InputStream is = hdfs.open(file);
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(is, "UTF-8"));
        StringBuilder builder = new StringBuilder();
        String line = null;
        while ((line = bufferedReader.readLine()) != null)
        {
            builder.append(line);
        }
        is.close();
        return builder.toString();
    }

    /**
     * Deletes file or directory
     *
     * @param path
     */
    public void delete(String path) throws IOException {
        Path file = new Path(rootPath + path);
        if (hdfs.exists(file)) {
            hdfs.delete(file, true);
        }
    }

    /**
     * Checks, is the "path" is directory or file
     *
     * @param path
     * @return
     */
    public boolean isDirectory(String path) throws IOException {
        Path file = new Path(rootPath + path);
        return hdfs.isDirectory(file);
    }

    /**
     * Return the list of files and subdirectories on any directory
     *
     * @param path
     * @return
     */
    public List<String> list(String path) throws IOException {
        Path file = new Path(rootPath + path);
        List<String> filePaths = new ArrayList<String>();
        //(heap issues with recursive approach) => using a queue
        Queue<Path> fileQueue = new LinkedList<Path>();

        //add the obtained path to the queue
        fileQueue.add(file);

        //while the fileQueue is not empty
        while (!fileQueue.isEmpty())
        {
            //get the file path from queue
            Path filePath = fileQueue.remove();

            //filePath refers to a file
            if (hdfs.isFile(filePath))
            {
                filePaths.add(filePath.toString());
            }
            else   //else filePath refers to a directory
            {
                //list paths in the directory and add to the queue
                FileStatus[] fileStatuses = hdfs.listStatus(filePath);
                for (FileStatus fileStatus : fileStatuses)
                {
                    fileQueue.add(fileStatus.getPath());
                } // for
            } // else

        } // while
        return filePaths;
    }
}