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