package org.cloudburst.etl.services;

import java.io.InputStream;
import java.util.List;

import org.cloudburst.etl.util.AWSManager;

public class TweetsDataStoreService {

    private static final String CMU_DATASETS_BUCKET = "cmucc-datasets";
    private static final String TWEETS_FOLDER = "twitter/f15/";

    private AWSManager awsManager;

	public TweetsDataStoreService(AWSManager awsManager) {
        this.awsManager = awsManager;
    }


    public List<String> getTweetFileNames() {
        return awsManager.listFiles(CMU_DATASETS_BUCKET, TWEETS_FOLDER);
    }

    public InputStream getTweetFileInputStream(String fileName) {
        return awsManager.getObjectInputStream(CMU_DATASETS_BUCKET, fileName);
    }

}
