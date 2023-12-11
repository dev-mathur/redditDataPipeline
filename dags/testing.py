import json

# Define the path to your JSON file
json_file_path = 'IngestionFiles/ProcessedData.json'

# Open the JSON file for reading
with open(json_file_path, 'r') as file:
    # Load the JSON object from the file
    json_object = json.load(file)

# Now you can use the json_object as a normal Python dictionary
#print(json_object)

#print(len(set(obj["data"]['post_hint'] for obj in json_object if 'post_hint' in obj)))
'''
for obj in json_object:
    if "data" in obj:
        print(obj["ingestion_timestamp"])
        print(obj["data"]["id"])
        print(obj["data"]["title"])
        print(obj["data"]["author"])
        print(obj["data"]["subreddit"])
        #print(obj["data"]["post_hint"])
        print(obj["data"]["num_comments"])
        print(obj["data"]["ups"])
        print(obj["data"]["downs"])
        print(obj["data"]["upvote_ratio"])
        print(obj["data"]["permalink"])
'''
# Define the input and output file paths
input_json_file_path = 'IngestionFiles/ConvertedRawData.json'
output_json_file_path = 'IngestionFiles/ProcessedData.json'

# Define the fields to extract (as a list of tuples where the first element is the outer key and the second is the inner key)
fields_to_extract = [('', 'ingestion_timestamp'), ('data', 'id'), ('data', 'title'), ('data', 'author'), ('data', 'subreddit'), ('data', 'post_hint'), ('data', 'num_comments'), ('data', 'ups'), ('data', 'downs'), ('data', 'upvote_ratio'), ('data', 'permalink')]

# Open the input file for reading and the output file for writing
with open(input_json_file_path, 'r') as input_file, open(output_json_file_path, 'w') as output_file:
    # Write the beginning of a JSON array to the output file
    output_file.write('[')

    # Initialize a variable to manage commas between objects
    first_object = True

    data = json.load(input_file)
    for obj in data:
        new_obj = {}
        for outer_key, inner_key in fields_to_extract:
            if outer_key:  # If there is an outer key, it's a nested field
                if outer_key in obj and inner_key in obj[outer_key]:
                    new_obj[inner_key] = obj[outer_key][inner_key]
            elif inner_key in obj:  # If there is no outer key, it's a top-level field
                new_obj[inner_key] = obj[inner_key]
            else:
                new_obj[inner_key] = 'None'
    
    # Write a comma before the next object if it's not the first
        if not first_object:
            output_file.write(',')
        else:
            first_object = False

        # Write the new JSON object to the output file
        json.dump(new_obj, output_file)
    # Write the end of the JSON array to the output file
    output_file.write(']')




################## IMPORT DATA #######################################
import zstandard as zstd
import json
import jsonlines
from datetime import datetime
from pymongo import MongoClient

################## EXTRACT DATA #######################################

#Load the initial data
zst = 'submissions.zst'

#Open and read the initial data
with open(zst, "rb") as f:
    data = f.read()

#Determine file name
date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
fileName = "IngestionFiles/ConvertedRawData.json"

#Decompress the input file
dctx = zstd.ZstdDecompressor()
decompressed = dctx.decompress(data, max_output_size=1000000000) # 1GB

#Write decompressed file content to json file with formatting changes
with open(fileName, "w+") as f:
    f.write("[" + decompressed.decode("utf-8").strip().replace("\n", ",").replace('"\n', '') + "]" )

#View objects
'''
with open(fileName) as f:
    data = json.load(f)
for d in data:
    print(d)'''

################## TRANSFORM DATA #######################################
processedFileName = 'IngestionFiles/ConvertedRawData.json'
output_json_file_path = 'IngestionFiles/ProcessedData.json'

# Define the fields to extract (as a list of tuples where the first element is the outer key and the second is the inner key)
fields_to_extract = [('', 'ingestion_timestamp'), ('data', 'id'), ('data', 'title'), ('data', 'author'), ('data', 'subreddit'), ('data', 'num_comments'), ('data', 'ups'), ('data', 'downs'), ('data', 'upvote_ratio'), ('data', 'permalink')]

#Create a list to store refined json objects
list = []

#Open decorator for Processed Raw JSON
with open(processedFileName) as redditPosts:
    #Iterate through posts
    for post in redditPosts:
        #Refine the JSON obj
        refinedPost = {
            "datePosted": post.ingestion_timestamp,
            'id': post.data.id,
            'title': post.data.title,
            'author': post.data.author,
            'subreddit': post.data.subreddit,
            'postType': post.data.post_hint if post.post.data.post_hint in post else 'None'
            'num_comments': post.data.num_comments,
            'num_upvotes': post.data.ups,
            'num_downvotes': post.data.downs,
            'upvote_ratio': post.data.upvote_ratio,
            'post_link': post.data.permalink
        }
        list.append(refinedPost)

'''
# Open the input file for reading and the output file for writing
with open(processedFileName, 'r') as input_file, open(output_json_file_path, 'w') as output_file:
    # Write the beginning of a JSON array to the output file
    output_file.write('[')

    # Initialize a variable to manage commas between objects
    first_object = True

    data = json.load(input_file)
    for obj in data:
        new_obj = {}
        for outer_key, inner_key in fields_to_extract:
            if outer_key:  # If there is an outer key, it's a nested field
                if outer_key in obj and inner_key in obj[outer_key]:
                    new_obj[inner_key] = obj[outer_key][inner_key]
            elif inner_key in obj:  # If there is no outer key, it's a top-level field
                new_obj[inner_key] = obj[inner_key]
            else:
                new_obj[inner_key] = 'None'
    
    # Write a comma before the next object if it's not the first
        if not first_object:
            output_file.write(',')
        else:
            first_object = False

        # Write the new JSON object to the output file
        json.dump(new_obj, output_file)
    # Write the end of the JSON array to the output file
    output_file.write(']')
'''


################## LOAD DATA #######################################
#Load mongo connection string
#client = MongoClient("mongodb://localhost:27017")

#If DB exists
    #If collection exists
        #Iteration logic
            #Insert many
#Else 
    #Create DB
    #db = client.RedditFiles
    #Create Collection
    #redditPosts = db.redditPosts
    #Iteration logic
        #Insert many
    #new_result = tutorial.insert_many([tutorial2, tutorial3])


#Iterate through processed data json
    #Add entry to mongodb table

