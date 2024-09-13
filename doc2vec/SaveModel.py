import re
import json
import sys, os, dotenv
import mysql.connector.pooling
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
from gensim.models.doc2vec import Doc2Vec, TaggedDocument

dotenv.load_dotenv(verbose=True)

class SaveModel:
    def __init__(self):
        self.host = os.environ['DATABASE_HOST']
        self.db = os.environ['DATABASE_NAME']
        self.user = os.environ['DATABASE_USER']
        self.password = os.environ['DATABASE_PASS']
        self.charset = 'utf8'
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
        pool = mysql.connector.pooling.MySQLConnectionPool(pool_name='mypool', pool_size=3, **dbConfig)
        con = pool.get_connection()
        cursor = con.cursor()
        """
            get verified user post
        """
        query = """
            SELECT
                Post.contents
            FROM
                Post
            LEFT JOIN
                UserGradeMap
            ON
                Post.userId = UserGradeMap.userId
            WHERE
                UserGradeMap.isVerified = 1
                OR UserGradeMap.isOg = 1
                OR UserGradeMap.isAdmin = 1
                OR UserGradeMap.isSuper = 1
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
    
    def saveModel(self):
        json_data = self.connection()
        resultList = list(map(lambda x: x['contents'], json_data))
        text = []
        for item in resultList:
            jsonObjectText = json.loads(item)
            blocks = jsonObjectText.get('blocks')
            
            if isinstance(blocks, list) :
                for block in blocks:
                    if block.get('data').get('text'):
                        text.append(block.get('data').get('text'))
            else:
                text.append(blocks.get('text'))
        """
            setup stopwords
        """
        stopWordsList = stopwords.words('english')
        f = open(self.root_dir + "/stopWords.txt", 'r')
        stopWordText = f.read().split(',')
        f.close()
        stopWords = set(stopWordsList)

        """
            words tokenize
        """
        sentText = sent_tokenize(str(text))
        normalizedText = [] 
        for sent in sentText: 
            tokens = re.sub("[^a-z]+"," ",sent.lower())
            normalizedText.append(tokens)
        result = [ word_tokenize(s) for s in normalizedText ]
        for i in range(len(result)):
            result[i] = [w for w in result[i] if w not in stopWords]
            result[i] = [w for w in result[i] if w not in stopWordText]
        f = open('words.txt', 'w')
        f.write(str(result))
        f.close()
        
        """
            Make word2vec model
        """
        print("start make word2vec model")
        tagged_data = [TaggedDocument(words=doc, tags=[str(idx)]) for idx, doc in enumerate(result)]
        model = Doc2Vec(vector_size= 32 , window = 5, min_count= 2 , workers = 4)
        model.build_vocab(tagged_data)
        model.train(tagged_data, total_examples=model.corpus_count, epochs=100)
        model.save(self.root_dir + "/doc2vec.model")
        return 
    
save = SaveModel()
save.saveModel()