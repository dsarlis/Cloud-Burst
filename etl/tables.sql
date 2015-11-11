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


CREATE TABLE IF NOT EXISTS q3 (
    userId BIGINT,
    creationTime DATE not null,
    impactScore BIGINT not null,
    tweetId BIGINT not null,
    text VARBINARY(1024) not null
) ENGINE = MyISAM;

create index userid_date_index on q3(userId, creationTime);

create index userid_impact_tweet on q3(impactScore, tweetId);

load data infile 'dump.csv' into table q3 fields terminated by '\t' lines terminated by '\n' SET text = UNHEX(text);
