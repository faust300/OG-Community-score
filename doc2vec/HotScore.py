#! /usr/bin/python3
import math, os, re, json, dotenv, timeit, time, decimal
import mysql.connector.pooling
from datetime import datetime
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from gensim.models.doc2vec import Doc2Vec
from gensim.models import Doc2Vec
import numpy as np
import timeit
dotenv.load_dotenv(verbose=True)

class HotScore():
    def __init__(self):
        self.host = os.environ['DATABASE_HOST']
        self.db = os.environ['DATABASE_NAME']
        self.user = os.environ['DATABASE_USER']
        self.password = os.environ['DATABASE_PASS']
        self.charset = 'utf8'
        self.model = Doc2Vec.load(os.path.dirname(os.path.abspath(__file__)) + '/doc2vec.model')
        self.stopWords = stopwords.words('english')
        self.root_dir = os.path.dirname(os.path.abspath(__file__))
        return
    
    def connection(self):
        dbConfig = {
            "host" : self.host,
            "db" : self.db,
            "user" : self.user,
            "password" : self.password,
            "charset" : self.charset,
        }
        pool = mysql.connector.pooling.MySQLConnectionPool(pool_name='mypool', pool_size=5, **dbConfig)
        return pool
    
    def calculate_weight(self, time_diff, decay_rate):
        weight = math.exp(-decay_rate * time_diff)
        return weight

    def get_time_difference(self, post_date):
        current_date = datetime.now()
        time_diff = (current_date - post_date).days
        return time_diff

    def calculate_hot_score(self, upvotes, viewCount, commentCount,  publish_time, similarity_scores, stanScore, gravity=1.1):
        view_weight = 40
        upvote_weight = 20
        comment_weight = 40
        score = (upvotes * upvote_weight) + (viewCount * view_weight) + (commentCount * comment_weight)
        deviation = np.std(stanScore)
        avg = np.average(stanScore)
        scoreDeviation = (decimal.Decimal(score) - avg) / deviation
        print(similarity_scores)
        print(scoreDeviation)
        print(decimal.Decimal(similarity_scores) + scoreDeviation)
        score = (scoreDeviation + decimal.Decimal(similarity_scores))
        return score
    
    def getPostId(self, pool):
        con = pool.get_connection()
        cursor = con.cursor()
        query = f"""
            SELECT
                Post.id,
                Post.contents,
                Post.viewCount,
                Post.commentCount,
                Post.upVoteCount,
                Post.createdAt
            FROM
                Post
            WHERE
                Post.userId != 0
                AND Post.deletedAt IS NULL
            ORDER BY
                Post.createdAt DESC
        """
        cursor.execute(query)
        row_headers=[x[0] for x in cursor.description]
        callQuery = cursor.fetchall()
        json_data=[]
        """
            parse json data
        """
        for result in callQuery:
            obj = dict(zip(row_headers, result))
            json_data.append(obj)
        cursor.close()
        con.close()
        return json_data
        
    def getPostDeviation(self, pool):
        con = pool.get_connection()
        cursor = con.cursor()
        query = f"""
            SELECT
                (viewCount * 0.4) + (commentCount * 0.5) + (upvoteCount * 0.7) AS score
            FROM
                Post
            WHERE
                Post.userId != 0
                AND Post.deletedAt IS NULL
            ORDER BY
                Post.createdAt DESC
        """
        cursor.execute(query)
        row_headers=[x[0] for x in cursor.description]
        callQuery = cursor.fetchall()
        json_data=[]
        """
            parse json data
        """
        for result in callQuery:
            obj = dict(zip(row_headers, result))
            json_data.append(obj['score'])
        cursor.close()
        con.close()
        return json_data
        
    

    def getPostContents(self, postId, pool):
        con = pool.get_connection()
        cursor = con.cursor()
        """
            get verified user post
        """
        query = f"""
            SELECT
                Post.id,
                Post.contents,
                Post.viewCount,
                Post.commentCount,
                Post.upVoteCount,
                Post.createdAt
            FROM
                Post
            WHERE
                Post.id = {postId}
            ORDER BY
                Post.createdAt DESC
        """
        cursor.execute(query)
        row_headers=[x[0] for x in cursor.description]
        callQuery = cursor.fetchall()
        json_data=[]
        """
            parse json data
        """
        for result in callQuery:
            json_data.append(dict(zip(row_headers,result)))
        cursor.close()
        con.close()
        
        return json_data


    def updatePostScore(self, update_many, pool):
        try:
            con = pool.get_connection()
            cursor = con.cursor()
            con.start_transaction()
            query = "UPDATE Post SET Post.score = %s WHERE Post.id = %s"
            cursor.executemany(query, update_many)
            con.commit()
        except Exception as e:
            con.rollback()
            print("rollback", e)            
        finally:
            cursor.close()
            con.close()
        return

    def calcScore(self, postId=0):
        all_start = timeit.default_timer()
        print('Start Post score :::::::')
        con = self.connection()
        id_list = []
        update_many = []
        if postId != 0:
            id_list = self.getPostContents(postId, con)
        else:
            get_post = self.getPostId(con)
            id_list = get_post
        for idx in range(len(id_list)):
            print(id_list[idx]['id'], "Post start")
            text = []
            jsonObjectText = json.loads(id_list[idx]['contents'])
            blocks = jsonObjectText.get('blocks')
            """
                words tokenization
            """
            if isinstance(blocks, list) :
                for block in blocks:
                    if block.get('data').get('text'):
                        text.append(block.get('data').get('text'))
            else:
                text.append(blocks.get('text'))
                    
            sentText = sent_tokenize(str(text))
            normalizedText = []
            f = open(self.root_dir + "/stopWords.txt", 'r')
            stopWordText = f.read().split(',')
            f.close()
            
            for sent in sentText: 
                tokens = re.sub("[^a-z]+"," ", sent.lower())
                for word in tokens.split():
                    normalizedText.append(word)
            user_post_words_token = [ word_tokenize(s) for s in normalizedText ]
            stopwords = set(self.stopWords)
            for i in range(len(user_post_words_token)):
                user_post_words_token[i] = [w for w in user_post_words_token[i] if w not in stopwords]
                user_post_words_token[i] = [w for w in user_post_words_token[i] if w not in stopWordText]
            score_array = []
            user_post_words_token = [item for item in user_post_words_token if item != []]
            for words in user_post_words_token:
                for word in words:
                    score_array.append(word)
            plusVector = 0
            minusVector = 0
            for words in score_array:
                try:
                    for item in self.model.wv[words]:
                        if item >= 0.5:
                            plusVector += 1
                        else:
                            minusVector += 1
                except:
                    pass
            avg = 0
            length = 0
            if len(score_array) < 7:
                length = len(score_array) * 100 * 32
            else :
                length = len(score_array) * 32
            if plusVector != 0:
                avg = (plusVector / length) * 100
            update_many.append((avg, id_list[idx]['id']))
            if len(update_many) >= 1000 or idx == len(id_list) - 1:
                update_post_start = timeit.default_timer()
                
                self.updatePostScore(update_many, con)
                update_post_end = timeit.default_timer()
                print(id_list[idx]['id'], "Post update end", update_post_end - update_post_start)
                update_many = []
            # self.updatePostScore(id_list[idx]['id'], avg, hot_score, con)
        print('Done')
        all_end = timeit.default_timer()
        print("All end", all_end - all_start, datetime.now())
        return True
    
hot = HotScore()
hot.calcScore()