#!/usr/bin/env python

import praw
import re
import sqlite3
from sqlite3 import Error

def get_submissions(sub):
    """ create a connection to the Reddit API and pull data
    :param sub: subreddit to pull submissions from
    :return: dictionary containing submission id as the key and the submission as the value
    """
    reddit = praw.Reddit('random_bot')
    submissions={}
    for submission in reddit.subreddit(sub).new():
        title = submission.title
        submissions[submission.id]=submission
    return submissions

def check_submission(conn, submission):
    """ checks to see if this submission has already been parsed or not
    :param submission_id: id of the submission to check
    :return: true if the submission has not been parsed yet, false otherwise 
    """
    cur = conn.cursor()
    sql = ''' SELECT COUNT(*) FROM submissions WHERE submission_id = ? '''
    cur.execute(sql, (submission.id,))     
    count = cur.fetchone()[0]
    if count < 1:
        print("Found a new submission")
        sql = '''INSERT INTO submissions (submission_id, created_time, title, url) \
            VALUES (?,?,?,?)'''
        cur.execute(sql, (submission.id,submission.created_utc,submission.title,submission.url))
        return True
    else:
        return False

def insert_submission(conn, submission):
    """ performs an insert or update to keep track of the count of words used in submission titles """
    cur = conn.cursor()
    words=submission.title.split()
    processed_words=process_words(words)
    for original_word, processed_word in zip(words,processed_words):
        if processed_word != "":
            sql = ''' INSERT OR REPLACE INTO words (original_word, processed_word, submission_id) \
                VALUES (?,?,?) '''
            cur.execute(sql, (original_word,processed_word,submission.id))

def process_words(words):
    """ performs text processing on words in a submission to standardize them for further analysis """
    processed_words = map(lambda x: x.lower(), words)
    processed_words = map(lambda x: re.sub('[^0-9a-zA-Z]+', '', x), processed_words)
    return processed_words

def record_submissions(subreddit):
    """ inserts data into the submissions and words tables """
    print("Getting submissions from r/{}...".format(subreddit))
    submissions=get_submissions(subreddit)
    conn=db_connection(database)
    with conn:
        print("Inserting new content into submissions and words tables...")
        for submission in submissions.values():
            if check_submission(conn, submission):
                insert_submission(conn, submission)
        print("Committing changes and closing the connection.")
        conn.commit()


def db_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return None
    
def main():
    record_submissions('stocks')
  
database="/Users/alex/Coding/reddit-bot/reddit_scraper.db"
if __name__== "__main__":
  main()

