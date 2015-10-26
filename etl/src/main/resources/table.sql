CREATE TABLE IF NOT EXISTS tweet (
    tweetId BIGINT primary key,
    usedId BIGINT,
    creationTime TIMESTAMP not null,
    text char(192) not null,
    score int not null default 0
) ENGINE = MyISAM;

create index creation_date_index on tweet(creationTime);