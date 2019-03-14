-- sqlite3 reddit_scraper.db ".read reddit_scraper.sql"
-- sqlite3 reddit_scraper.db "select * from words"
-- sqlite3 reddit_scraper.db "select * from posts;select * from words;"

DROP TABLE IF EXISTS posts;
CREATE TABLE posts (
post_id VARCHAR(10),
created_time TIME,
title VARCHAR(300),
url VARCHAR(300)
);

DROP TABLE IF EXISTS words;
CREATE TABLE words (
word VARCHAR(100),
count INT DEFAULT 1,
PRIMARY KEY (word)
);