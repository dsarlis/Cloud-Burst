CREATE TABLE IF NOT EXISTS tweets (
    tweetId BIGINT primary key,
    userId BIGINT not null,
    creationTime TIMESTAMP not null,
    followersCount int not null default 0,
    score int not null default 0,
    text VARBINARY(1024) not null
) ENGINE = MyISAM;

CREATE TABLE IF NOT EXISTS hashtags (
    hashtag VARBINARY(1024) not null,
    tweetId BIGINT not null,
    count int not null,
    PRIMARY KEY (hashtag, tweetId)
) ENGINE = MyISAM;


create index userid_creation_date_index on tweets(userId, creationTime);


load data infile 'dump.csv' into table tweets fields terminated by ',' lines terminated by '\n' SET text = UNHEX(text);

/* Q4 Loading into mySql */
	CREATE TABLE IF NOT EXISTS hashtags (
    hashtag VARBINARY(990) not null,
    createdAtDate DATE not null,
    totalHashTagCount INT not null,
    sortedUniqueUserList VARCHAR(1024) not null,
    originTweetText VARBINARY(1024) not null
) ENGINE = MyISAM;

create index hashTag_createdAtDate_totalHashTagCount on hashtags(hashtag, createdAtDate, totalHashTagCount);

LOAD DATA LOCAL INFILE 'concatAllFiesl' 
INTO TABLE hashtags
FIELDS TERMINATED BY '\t' SET hashtag = UNHEX(hashtag) , originTweetText = UNHEX(originTweetText) ;
/* Ends here */