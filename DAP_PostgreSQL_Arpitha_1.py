import certifi
import psycopg2
import pymongo

from pymongo.mongo_client import MongoClient
# Replace the placeholder with your Atlas connection string
uri = "mongodb+srv://arpitharao0806:f9tYKfkA0bxdTIPa@cluster0.6donncf.mongodb.net/?retryWrites=true&w=majority"
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
    password='Avyay0806@'
)

if conn :
    print('connected')
else:
    print('false')

print(conn)


cur = conn.cursor()
cur.execute("""
        CREATE TABLE IF NOT EXISTS Thyroid (
_id VARCHAR(255),
age VARCHAR(255),
sex VARCHAR(255),
on_thyroxine VARCHAR(255),
query_on_thyroxine VARCHAR(255),
on_antithyroid_medication VARCHAR(255),
sick VARCHAR(255),
pregnant VARCHAR(255),
thyroid_surgery VARCHAR(255),
I131_treatment VARCHAR(255),
query_hypothyroid VARCHAR(255),
query_hyperthyroid VARCHAR(255),
lithium VARCHAR(255),
goitre VARCHAR(255),
tumor VARCHAR(255),
hypopituitary VARCHAR(255),
psych VARCHAR(255),
TSH_measured  VARCHAR(255),
data111 VARCHAR(255),
T3_measured VARCHAR(255),
data_1  VARCHAR(255),
TT4_measured VARCHAR(255),
TT4 VARCHAR(255),
T4U_measured VARCHAR(255),
T4U VARCHAR(255),
FTI_measured VARCHAR(255),
FTI VARCHAR(255),
TBG_measured VARCHAR(255),
referral_source VARCHAR(255),
binaryClass VARCHAR(255)
    )
""")
conn.commit()

cur.execute("""
    TRUNCATE TABLE Thyroid
""")
conn.commit()


for mongo_doc in db.Processed_Thyroid.find():
    postgresql_data = (
        mongo_doc.get('_id'),
        mongo_doc.get('age'),
        mongo_doc.get('sex'),
        mongo_doc.get('on_thyroxine'),
        mongo_doc.get('on_antithyroid_medication'),
        mongo_doc.get('sick'),
        mongo_doc.get('pregnant'),
        mongo_doc.get('thyroid_surgery'),
        mongo_doc.get('I131_treatment'),
        mongo_doc.get('query_hypothyroid'),
        mongo_doc.get('query_hyperthyroid'),
        mongo_doc.get('lithium'),
        mongo_doc.get('goitre'),
        mongo_doc.get('tumor'),  # convert dict to string
        mongo_doc.get('hypopituitary'),
        mongo_doc.get('psych'),
        mongo_doc.get('TSH_measured'),
        mongo_doc.get('data111'),
        mongo_doc.get('T3_measured'),
        mongo_doc.get('data_1'),
        mongo_doc.get('TT4_measured'),
        mongo_doc.get('TT4'),
        mongo_doc.get('T4U_measured'),
        mongo_doc.get('T4U'),
        mongo_doc.get('FTI_measured'),
        mongo_doc.get('FTI'),
        mongo_doc.get('TBG_measured'),
        mongo_doc.get('referral_source'),
        mongo_doc.get('binaryClass')


    )
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO Thyroid 
        (
_id,
age,
sex,
on_thyroxine,
query_on_thyroxine,
on_antithyroid_medication,
sick,
pregnant,
thyroid_surgery,
I131_treatment,
query_hypothyroid,
query_hyperthyroid,
lithium,
goitre,
tumor,
hypopituitary,
psych,
TSH_measured,
data111,
T3_measured,
data_1,
TT4_measured,
TT4, 
T4U_measured,
T4U,
FTI_measured,
FTI,
TBG_measured,
referral_source,
binaryClass
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