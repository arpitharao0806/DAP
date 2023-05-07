import certifi
import psycopg2
import pymongo

from pymongo.mongo_client import MongoClient
# Replace the placeholder with your Atlas connection string
uri = "mongodb+srv://vinithvkk050:JZNwTd9eOyQLC6Kk@cluster0.lrjammo.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
client = MongoClient(uri, tlsCAFile=certifi.where())
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

db = client["dataset"]

conn = psycopg2.connect(
    host='localhost',
    database='postgres',
    user='postgres',
    password='DevilLucifer@98'
)

if conn :
    print('connected')
else:
    print('false')

print(conn)


cur = conn.cursor()
cur.execute("""
        CREATE TABLE IF NOT EXISTS Thyroid (
Class VARCHAR(255),
FTI VARCHAR(255),
I131_treatment VARCHAR(255),
T3 VARCHAR(255),
T4U VARCHAR(255),
TSH VARCHAR(255),
TT4 VARCHAR(255),
age VARCHAR(255),
goitre VARCHAR(255),
hypopituitary VARCHAR(255),
lithium VARCHAR(255),
on_antithyroid_medication VARCHAR(255),
on_thyroxine VARCHAR(255),
pregnant VARCHAR(255),
psych VARCHAR(255),
query_hyperthyroid VARCHAR(255),
query_hypothyroid VARCHAR(255),
query_on_thyroxine  VARCHAR(255),
referral_source VARCHAR(255),
sex VARCHAR(255),
sick  VARCHAR(255),
thyroid_surgery VARCHAR(255),
tumor VARCHAR(255)
    )
""")
conn.commit()

cur.execute("""
    TRUNCATE TABLE Thyroid
""")
conn.commit()


for mongo_doc in db.Processed_Thyroid.find():
    postgresql_data = (
        mongo_doc.get('Class'),
        mongo_doc.get('FTI'),
        mongo_doc.get('I131_treatment'),
        mongo_doc.get('T3'),
        mongo_doc.get('T4U'),
        mongo_doc.get('TSH'),
        mongo_doc.get('TT4'),
        mongo_doc.get('age'),
        mongo_doc.get('goitre'),
        mongo_doc.get('hypopituitary'),
        mongo_doc.get('lithium'),
        mongo_doc.get('on_antithyroid_medication'),
        mongo_doc.get('on_thyroxine'),
        mongo_doc.get('pregnant'),  # convert dict to string
        mongo_doc.get('psych'),
        mongo_doc.get('query_hyperthyroid'),
        mongo_doc.get('query_hypothyroid'),
        mongo_doc.get('query_on_thyroxine'),
        mongo_doc.get('referral_source'),
        mongo_doc.get('sex'),
        mongo_doc.get('sick'),
        mongo_doc.get('thyroid_surgery'),
        mongo_doc.get('tumor')
    )
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO Thyroid 
        (
                   Class,
FTI,
I131_treatment,
T3,
T4U,
TSH,
TT4,
age,
goitre,
hypopituitary,
lithium,
on_antithyroid_medication,
on_thyroxine,
pregnant,
psych,
query_hyperthyroid,
query_hypothyroid,
query_on_thyroxine,
referral_source,
sex,
sick,
thyroid_surgery,
tumor 
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        """, postgresql_data)
    conn.commit()