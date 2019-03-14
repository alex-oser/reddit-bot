#!/usr/bin/env python

import praw
import sqlite3
from sqlite3 import Error

def get_posts(sub):
    """ create a connection to the Reddit API and pull data
    :param sub: subreddit to pull posts from
    :return: dictionary containing post id as the key and the post as the value
    """
    reddit = praw.Reddit('random_bot')
    posts={}
    for post in reddit.subreddit(sub).new(limit=10):
        submission = reddit.submission(id="{}".format(post))
        title = submission.title
        posts[submission.id]=submission
    return posts

def check_post(conn, post_id, post):
    """ checks to see if this post has already been parsed or not
    :param post_id: id of the post to check
    :return: true if the post has not been parsed yet, false otherwise 
    """
    cur = conn.cursor()
    sql = ''' SELECT COUNT(*) FROM posts WHERE post_id = ? '''
    cur.execute(sql, (post_id,))     
    numRows = cur.fetchone()[0]
    if numRows < 1:
        print("Found a new post")
        sql = '''INSERT INTO posts (post_id, created_time, title, url) \
            VALUES (?,?,?,?)'''
        cur.execute(sql, (post_id,post.created_utc,post.title,post.url))
        return True
    else:
        return False

def insert_post(conn, words):
    """ performs an insert or update to keep track of the count of words used in post titles """
    cur = conn.cursor()
    for word in words:
        sql = ''' INSERT OR REPLACE INTO words (word, count) \
            VALUES (?, COALESCE((SELECT count FROM words WHERE word=?)+1,1)) '''
        cur.execute(sql, (word,word))

def record_posts(subreddit):
    """ inserts data into the posts and words tables """
    print("Getting posts from r/{}...".format(subreddit))
    posts=get_posts(subreddit)
    conn=db_connection(database)
    with conn:
        print("Inserting new content into posts and words tables...")
        for post_id, post in posts.items():
            if check_post(conn, post_id, post):
                words=post.title.split()
                insert_post(conn, words)
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
    record_posts('all')
  
database="/Users/alex/Coding/praw/reddit_scraper.db"
if __name__== "__main__":
  main()

