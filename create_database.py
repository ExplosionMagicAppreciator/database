import sqlite3
import click
import json 
from datetime import datetime


sql_transaction = []
start_row = 0
cleanup = 100000

connection = sqlite3.connect('name.db')
cursor = connection.cursor()

def create_table():
    query = """CREATE TABLE IF NOT EXISTS reddit_conversations(
        parent_id TEXT PRIMARY KEY,
        comment_id TEXT UNIQUE,
        parent TEXT,
        comment TEXT,
        subreddit TEXT,
        conversation TEXT,
        unix INT,
        score INT
        )"""
    cursor.execute(query)
    return True


def format_data(data):
    #clean data
    data = data.replace('\n',' [newlinechar] ')
    data = data.replace('\r', ' [newlinechar] ')
    data = data.replace('"', "'")


def data_is_acceptable(data):
    if  len(data) < 1:
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True


def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        cursor.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                cursor.execute(s)
            except:
                pass
        connection.commit()
        sql_transaction = []




def find_existing_score(id):
    try:
        sql = "SELECT score FROM reddit_conversations WHERE parent_id ='{}'".format (id)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        #print('find parent', e)
        return False
    
def find_parent(pid):
    try:
        sql = "SELECT comment FROM reddit_conversations WHERE comment_id = '{}'".format (pid)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        #print('find parent', e)
        return False
    
        
def sql_insert_replace_comment(commentid,parentid,parent,
                               comment,subreddit,time,score,root):
    try:
        sql = """UPDATE reddit_conversations SET parent_id = ?,
        comment_id = ?,
        parent = ?,
        comment = ?,
        subreddit = ?,
        conversation = ?,
        unix = ?,
        score = ? WHERE parent_id =?;""".format(parentid,commentid, parent,comment,
                                                subreddit,root, int(time), score, parentid)
        transaction_bldr(sql)
    except Exception as e:
        print('replace insertion',str(e))

def sql_insert_has_parent(commentid,parentid,parent,
                          comment,subreddit,time,score,root):
    try:
        sql = """INSERT INTO reddit_conversations (parent_id,
        comment_id,
        parent,
        comment,
        subreddit,
        conversation,
        unix,
        score) VALUES ("{}","{}","{}","{}","{}","{}","{}","{}");""".format(parentid, commentid, parent, comment,
                                
                                                                  subreddit, root, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('has parent insertion',str(e))

def sql_insert_no_parent(commentid,parentid,comment,
                         subreddit,time,score,root):
    try:
        sql = """INSERT INTO reddit_conversations (parent_id,
        comment_id,
        comment,
        subreddit,
        conversation,
        unix,
        score) VALUES ("{}","{}","{}","{}","{}","{}","{}");""".format(parentid, commentid, comment,
                                                             subreddit,root, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('no parent insertion',str(e))

    


if __name__== '__main__': #creates the table if it doesn't already exists
    create_table()
    if create_table():
        row_counter = 0
        paired_rows = 0


        with open('',
                    buffering=1000) as f:
                #rows = len(f.readlines())
                for row in f:
                    #print(row)
                    row_counter += 1 
                    #print(row_counter)
                    if row_counter > start_row:
                        try:
                            row = json.loads(row)
                            meta = row['meta']
                            parent_id = row['reply-to']
                            body = row['text']
                            timestamp = row['timestamp']
                            score = meta['score']
                            subreddit = meta['subreddit']
                            comment_id = row['id']
                            conversation = row['root']
                            parent_data = find_parent(parent_id)
                            existing_comment_score = find_existing_score(parent_id)
                            #print(parent_id, body, timestamp, subreddit, conversation)
                            #print(parent_data)

                            if existing_comment_score:
                                    if score > existing_comment_score:
                                        if data_is_acceptable(body):
                                            sql_insert_replace_comment(comment_id,parent_id,parent_data,body,subreddit,timestamp,score,conversation)            
                            else:
                                if data_is_acceptable(body):
                                    if parent_data:
                                            
                                            sql_insert_has_parent(comment_id,parent_id,parent_data,body,subreddit,timestamp,score,conversation)
                                            paired_rows += 1
                                    else:
                                        sql_insert_no_parent(comment_id,parent_id,body,subreddit,timestamp,score,conversation)
                        except Exception as e:
                            print(str(e))
                            
                    if row_counter % 1000 == 0:
                        click.clear()
                        print('{} Rows read, Paired Rows: {}, Time: {}'.format(row_counter, paired_rows, str(datetime.now())))

                    if row_counter > start_row:
                        if row_counter % cleanup == 0:
                            print("Cleanin up!")
                            sql = "DELETE FROM reddit_conversations WHERE parent IS NULL"
                            cursor.execute(sql)
                            connection.commit()
                            cursor.execute("VACUUM")
                            connection.commit()
                    