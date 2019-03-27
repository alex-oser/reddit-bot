#!/usr/bin/env python

import csv
import praw
import re
import os
from pathlib import Path
import requests
import sqlite3
from sqlite3 import Error
import sys


def environment_setup():
    """ performs environment validations before the script proceeds """
    script_path=os.path.dirname(os.path.realpath(__file__))
    config_file=Path('{}/praw.ini'.format(script_path))
    if not config_file.is_file():
        sys.exit('Error: praw.ini does exist. Unable to configure praw connection. Please create the praw.ini file.')
    database_file=Path(database_path)
    if not database_file.is_file():
        print("Database does not exist, initializing a new one.")
        create_database()

def create_database():
    """ creates database if it does not already exist """
    f=open("".join([script_path,"/reddit_parser.sql"]), 'r')
    sqlFile=f.read()
    f.close()
    sqlCommands=sqlFile.split(';')
    conn=db_connection(database_path)
    with conn:
        cur = conn.cursor()
        for command in sqlCommands:
            try:
                cur.execute(command)
            except Error as e:
                print(e)


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

def check_object(conn, praw_object):
    """ checks to see if this object has already been parsed or not
    if not, inserts it into the objects database
    :param praw_object: the object to check (either a submission or a comment)
    :return: True if the object has not been parsed yet, False if not 
    """
    cur = conn.cursor()
    sql = ''' SELECT COUNT(*) FROM objects WHERE id = ? '''
    cur.execute(sql, (praw_object.fullname,))     
    count = cur.fetchone()[0]
    if count < 1:
        praw_object_type = get_object_type(praw_object.fullname)
        sql,values = get_object_values(praw_object, praw_object_type)
        cur.execute(sql, values)
        return True
    else:
        return False

def check_num_comments(conn, submission):
    """ checks to see if the number of comments has changed since the last time the post was processed 
    :param conn: database connection object
    :param submission: submission to check
    :return: True if the number of comments has changed, False if not
    """
    cur = conn.cursor()
    sql = ''' SELECT num_comments FROM objects WHERE id = ? '''
    cur.execute(sql, (submission.fullname,))     
    num_comments = cur.fetchone()
    if num_comments == None:
        return True
    elif submission.num_comments != num_comments[0]:
        return True
    else:
        return False

def get_object_values(praw_object, praw_object_type):
    """ returns the sql and values to insert based on the object type
    :param praw_object: object to get the values for
    :param praw_object_type: type of object
    :return sql: sql statement to use to insert to objects table
    :return values: values to use to insert to objects table 
    """
    if praw_object_type == "Comment":
        sql = '''INSERT INTO objects (id, type, created_time, content, url) VALUES (?,?,?,?,?)'''
        values = (praw_object.fullname,praw_object_type,praw_object.created_utc,praw_object.body,praw_object.permalink)
    elif praw_object_type == "Link":
        sql = '''INSERT INTO objects (id, type, created_time, content, url, num_comments) VALUES (?,?,?,?,?,?)'''
        values = (praw_object.fullname,praw_object_type,praw_object.created_utc,praw_object.title,praw_object.url,praw_object.num_comments)
    return sql, values
    

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
    num_comments_has_changed = check_num_comments(conn, submission)
    if not num_comments_has_changed:
        return
    if check_object(conn, submission):
        insert_words(conn, words, submission.fullname)
    submission.comments.replace_more(limit=None)
    for comment in submission.comments.list():
        if check_object(conn, comment):
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
    num_submissions=len(submissions)
    conn=db_connection(database_path)
    count=0
    with conn:
        print("Inserting new content into submissions and words tables...")
        for submission in submissions.values():
            count+=1
            sys.stdout.write("\rProcessing submission {}/{}...".format(count,num_submissions))
            # if check_object(conn, submission): # For now, moving check here to the object level instead of submission level, this will
            # re-check submissions for new comments
            process_submission(conn, submission)
        print("\nCommitting changes and closing the connection.")
        conn.commit()

def record_stock_info():
    with requests.Session() as s:
        amex = s.get('https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download')
        nyse = s.get('https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download')
        nasdaq = s.get('https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download')
        amex_csv = csv.reader(amex.content.decode('utf-8').splitlines(), delimiter=',')
        nyse_csv = csv.reader(nyse.content.decode('utf-8').splitlines(), delimiter=',')
        nasdaq_csv = csv.reader(nasdaq.content.decode('utf-8').splitlines(), delimiter=',')
        csv.reader
    conn=db_connection(database_path)
    with conn:
        cur = conn.cursor()
        stmt = ''' INSERT INTO stock_info (symbol, name, last_sale, market_cap, ipo_year, sector, industry, summary_quote) \
            VALUES (?,?,?,?,?,?,?,?) '''
        cur.executemany(stmt, amex_csv)



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
    environment_setup()
    record_submissions('stocks')

script_path=os.path.dirname(os.path.realpath(__file__))
database="reddit_parser.db"
database_path="{}/{}".format(script_path,database)
if __name__== "__main__":
  main()

