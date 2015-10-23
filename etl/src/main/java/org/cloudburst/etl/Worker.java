package org.cloudburst.etl;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.cloudburst.etl.services.TweetsDataStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Queue;

public class Worker implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(Worker.class);

    private Queue<String> fileNamesQueue;
    private TweetsDataStoreService tweetsDataStoreService;

    public Worker(Queue<String> fileNamesQueue, TweetsDataStoreService tweetsDataStoreService) {
        this.fileNamesQueue = fileNamesQueue;
        this.tweetsDataStoreService = tweetsDataStoreService;
    }

    public void run() {
        while(fileNamesQueue.size() > 0) {
            String fileName = fileNamesQueue.poll();

            if (fileName != null) {
                InputStream inputStream = tweetsDataStoreService.getTweetFileInputStream(fileName);

                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line = null;

                try {
                    while((line = reader.readLine()) != null) {
                        JsonElement jsonElement = new JsonParser().parse(line);
                        JsonObject  jsonObject = jsonElement.getAsJsonObject();
                        DateFormat ISO8601Parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");


                    }

                } catch (IOException ex) {
                    logger.error("Problem reading object", ex);
                }

            }
        }
    }
}
