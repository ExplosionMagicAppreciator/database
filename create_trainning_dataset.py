import sqlite3
import pandas as pd 



connection = sqlite3.connect('c:/Users/JJ/Downloads/Compressed/reddit-corpus/utterances.db')
cursor = connection.cursor()
limit = 5000
last_unix = 0
cursor_length = limit 
counter = 0
test_done = False


def format_data(data):
    #clean data 
    data = str(data)
    data = data.replace('\n',' [newline] ')
    data = data.replace('\r', ' [newline] ')
    data = data.replace('"', "'")
    data = data.replace('&gt; ', '')
    data = data.replace('&gt;', '')
    return data


query = """SELECT * FROM reddit_conversations WHERE unix > {}
                 ORDER BY unix ASC LIMIT {}"""
df = pd.read_sql(query.format(last_unix,limit), connection, )
last_unix = df.tail(1)['unix'].values[0]
cursor_length = len(df)
with open('train.from','a', encoding='utf8') as f:
    for content in df['parent'].values:
        f.write(format_data(content)+'\n')
with open('train.to','a', encoding='utf8') as f:
    for content in df['comment'].values:
        f.write(format_data(content)+'\n')
counter += 1
if counter % 20 == 0:
    print(counter*limit, 'rows completed so far')


            

                
