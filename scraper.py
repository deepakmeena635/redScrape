from IPython.display import clear_output
import time 
from tqdm import tqdm
import praw 
import pandas as pd
import datetime as dt
from psaw import PushshiftAPI
import os
import pickle 
import datetime as dt


reddit = praw.Reddit(client_id='7yjPl4FzmiQm1pbaTcF19Q', client_secret='3hcuOb_Eo8J_cbre7EECny9oq1l10A', user_agent='idek', check_for_async=False )
api = PushshiftAPI(reddit)

lcl = "./roastMe/"
if "data.txt" not in os.listdir(lcl):
      data = set()
else: 
  with open(lcl+'data.txt', 'rb') as file: 
    data = pickle.load(file)

start_epoch=int(dt.datetime(2015, 1, 1).timestamp())
submissions = list(api.search_submissions(after=start_epoch,
                                  sort_type='num_comments',
                                  subreddit='RoastMe',
                                  filter = ['url','author','title', 'subreddit'],limit=100000))


index =0 
pdlen= 0 
subreddit = reddit.subreddit('RoastMe')
with open(lcl+"all_comments.csv", 'a+') as file :
  for submission in tqdm(submissions):
    # print("submission #{} total comments{}".format(index, len(data)), end=" ")
    if submission.id in data:
      if index % 100 ==0: 
        # print('\r'*30) 
      index+=1 
      continue
    submission.comments.replace_more(limit=0)
    temp  = [[ submission.id, com.id, com.body, len(com.body), com.ups, com.total_awards_received ] for com in submission.comments]
    data.add(str(submission.id))
    file.write("<end_com>\n".join([",".join([str(j) for j in i]) for i in temp]))
    index +=1 
    if index > pdlen+15:
      pdlen = index
      with open(lcl+"data.txt", 'wb') as pkl: 
        pickle.dump(data, pkl )
      
    # print('\r'*30) 