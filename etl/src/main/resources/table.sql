CREATE TABLE IF NOT EXISTS tweet (
    tweetId BIGINT primary key,
    usedId BIGINT unique,
    creationTime TIMESTAMP not null,
    text varchar(256) not null,
    score int not null default 0
);