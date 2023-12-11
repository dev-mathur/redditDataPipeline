################## IMPORT DATA #######################################
import zstandard as zstd
import json
from datetime import datetime
from pymongo import MongoClient
import gdown

def extract(**kwargs):
    execution_date = kwargs['execution_date']
    ################## EXTRACT DATA #######################################
    # a file
    url = "https://drive.google.com/uc?id=1E7iRwCp7IjvCjh_-owrt2NTMWnvgleZp"
    output = "submissions.zst"
    gdown.download(url, output, quiet=False)

    #Open and read the initial data
    zst = 'submissions.zst'

    with open(zst, "rb") as f:
        data = f.read()

    #Determine file name
    fileName = "ConvertedRawData.json"

    #Decompress the input file
    dctx = zstd.ZstdDecompressor()
    decompressed = dctx.decompress(data, max_output_size=1000000000) # 1GB

    #Write decompressed file content to json file with formatting changes
    with open(fileName, "w+") as f:
        f.write("[" + decompressed.decode("utf-8").strip().replace("\n", ",").replace('"\n', '') + "]" )
    print(fileName)

    return fileName

    ################## TRANSFORM DATA #######################################
def transform(fileName):
    #Create a list to store refined json objects
    list = []

    #Open decorator for Processed Raw JSON
    with open(fileName) as file:
        #Convert to JSON object
        redditPosts = json.load(file)
        
        #Iterate through posts
        for post in redditPosts:
            #Refine the JSON obj
            refinedPost = {
                "datePosted": post['ingestion_timestamp'],
                'id': post['data']['id'],
                'title': post['data']['title'],
                'author': post['data']['author'],
                'subreddit': post['data']['subreddit'],
                'postType': post['data']['post_hint'] if 'post_hint' in post['data'] else 'None',
                'num_comments': post['data']['num_comments'],
                'num_upvotes': post['data']['ups'],
                'num_downvotes': post['data']['downs'],
                'upvote_ratio': post['data']['upvote_ratio'],
                'post_link': post['data']['permalink']
            }
            list.append(refinedPost)
    return list
 
    ################## LOAD DATA #######################################
def load(list):
    try: 
        client = MongoClient("mongodb://mongo:27017/") 
        print("Connected successfully!!!") 
    except:   
        print("Could not connect to MongoDB") 
    
    # Check if the database exists
    if 'redditDB' not in client.list_database_names():
        db = client['redditDB']
    else:
        db = client['redditDB']
    
    # Check if the collection exists
    if 'redditPosts' not in db.list_collection_names():
        collection = db.create_collection('redditPosts')
    else:
        collection = db['redditPosts']

    cursor = collection.find({})
    for document in cursor:
          print(document)

    #Close connection
    client.close()