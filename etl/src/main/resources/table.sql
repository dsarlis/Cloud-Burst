CREATE TABLE IF NOT EXISTS tweets (
    tweetId BIGINT primary key,
    userId BIGINT not null,
    creationTime TIMESTAMP not null,
    text varchar(1024) not null,
    score int not null default 0
) ENGINE = MyISAM;

create index userid_creation_date_index on tweets(userId, creationTime);