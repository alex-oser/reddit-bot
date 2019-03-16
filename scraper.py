#!/usr/bin/env python

import praw
import re
import sqlite3
from sqlite3 import Error
import sys

def get_submissions(sub):
    """ create a connection to the Reddit API and pull data
    :param sub: subreddit to pull submissions from
    :return: dictionary containing submission id as the key and the submission as the value
    """
    reddit = praw.Reddit('random_bot')
    submissions={}
    for submission in reddit.subreddit(sub).new(limit=10):
        title = submission.title
        submissions[submission.id]=submission
    return submissions

def check_object(conn, praw_object):
    """ checks to see if this object has already been parsed or not
    :param submission_id: id of the object to check
    :return: true if the object has not been parsed yet, false otherwise 
    """
    cur = conn.cursor()
    sql = ''' SELECT COUNT(*) FROM objects WHERE id = ? '''
    cur.execute(sql, (praw_object.fullname,))     
    count = cur.fetchone()[0]
    if count < 1:
        print("Found a new object")
        praw_object_type = get_object_type(praw_object.fullname)
        sql = '''INSERT INTO objects (id, type, created_time, content, url) \
            VALUES (?,?,?,?,?)'''
        values=get_object_values(praw_object, praw_object_type)
        cur.execute(sql, values)
        return True
    else:
        return False

def get_object_values(praw_object, praw_object_type):
    """ insert object information to the objects table """
    if praw_object_type == "Comment":
        values = (praw_object.fullname,praw_object_type,praw_object.created_utc,praw_object.body,praw_object.permalink)
    elif praw_object_type == "Link":
        values = (praw_object.fullname,praw_object_type,praw_object.created_utc,praw_object.title,praw_object.url)
    return values
    

def get_object_type(object_id):
    """returns the type of object per https://www.reddit.com/dev/api/"""
    object_types = {
        "t1": "Comment",
        "t2": "Account",
        "t3": "Link",
        "t4": "Message",
        "t5": "Subreddit",
        "t6": "Award"
    }
    return object_types[object_id[:2]]

def process_submission(conn, submission):
    """ get the words from each object in the submission and insert them """
    words = submission.title.split()
    insert_words(conn, words, submission.fullname)
    for comment in submission.comments:
        words = comment.body.split()
        insert_words(conn, words, comment.fullname)

def process_words(words):
    """ performs text processing on words in a submission to standardize them for further analysis """
    processed_words = map(lambda x: x.lower(), words)
    processed_words = map(lambda x: re.sub('[^0-9a-zA-Z]+', '', x), processed_words)
    return processed_words

def insert_words(conn, words, object_id):
    """ performs an insert or update to keep track of the count of words used in submission titles """
    cur = conn.cursor()
    processed_words=process_words(words)
    for original_word, processed_word in zip(words,processed_words):
        if processed_word != "":
            sql = ''' INSERT INTO words (original_word, processed_word, object_id) \
                VALUES (?,?,?) '''
            cur.execute(sql, (original_word,processed_word,object_id))

def record_submissions(subreddit):
    """ inserts data into the submissions and words tables """
    print("Getting submissions from r/{}...".format(subreddit))
    submissions=get_submissions(subreddit)
    conn=db_connection(database)
    with conn:
        print("Inserting new content into submissions and words tables...")
        for submission in submissions.values():
            if check_object(conn, submission):
                process_submission(conn, submission)
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

