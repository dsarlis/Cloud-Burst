package org.cloudburst.etl.services;

import java.io.InputStream;
import java.util.List;

import org.cloudburst.etl.util.AWSManager;

public class TweetsDataStoreService {

    private static final String CMU_DATASETS_BUCKET = "cmucc-datasets";
    private static final String TWEETS_FOLDER = "twitter/f15/";
    private static final String REDCUDED_TWEETS_BUCKET_NAME = "reducedtweets";

    private AWSManager awsManager;

	public TweetsDataStoreService(AWSManager awsManager) {
        this.awsManager = awsManager;
    }


    public List<String> getTweetFileNames() {
        List<String> fileNames = awsManager.listFiles(CMU_DATASETS_BUCKET, TWEETS_FOLDER);

        fileNames.remove("twitter/f15/_SUCCESS");
        return fileNames;
    }

    public InputStream getTweetFileInputStream(String fileName) {
        return awsManager.getObjectInputStream(CMU_DATASETS_BUCKET, fileName);
    }

    public void saveTweetsFile(String fileName) {
        awsManager.storeInBucket(fileName, REDCUDED_TWEETS_BUCKET_NAME, fileName);
    }

}
