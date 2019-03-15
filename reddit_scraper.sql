-- sqlite3 reddit_scraper.db ".read reddit_scraper.sql"
-- sqlite3 reddit_scraper.db "select * from words"

DROP TABLE IF EXISTS submissions;
CREATE TABLE submissions (
	submission_id VARCHAR(10),
	created_time INT,
	title VARCHAR(300),
	url VARCHAR(300)
);

DROP TABLE IF EXISTS words;
CREATE TABLE words (
	original_word VARCHAR(100),
	processed_word VARCHAR(100),
	submission_id VARCHAR(10),
	timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
	FOREIGN KEY(submission_id) REFERENCES submissions(submission_id)
);

DROP VIEW IF EXISTS proc_sum;
CREATE VIEW proc_sum AS
	SELECT processed_word, count(*) as count
	FROM words
	GROUP BY processed_word
	ORDER BY count DESC;
