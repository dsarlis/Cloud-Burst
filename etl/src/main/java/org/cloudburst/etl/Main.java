package org.cloudburst.etl;

import org.cloudburst.etl.services.TweetsDataStoreService;
import org.cloudburst.etl.util.AWSManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Main {

    private final static Logger logger = LoggerFactory.getLogger(Main.class);
    private final static int THREAD_NUMBER = 16;

    public static void main(String[] args) {
        TweetsDataStoreService tweetsDataStoreService = new TweetsDataStoreService(new AWSManager());
        List<String> tweetFileNames = tweetsDataStoreService.getTweetFileNames();
        Queue<String> fileNamesQueue = new ConcurrentLinkedQueue<String>(tweetFileNames);

        //Start workers
    }

}
