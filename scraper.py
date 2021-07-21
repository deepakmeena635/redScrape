import time 
from tqdm import tqdm
import praw 
import pandas as pd
import datetime as dt
from psaw import PushshiftAPI
import os
import pickle
import datetime as dt
import io
from praw.models import MoreComments
import matplotlib.pyplot as plt 
import numpy as np
import cv2
import requests

reddit = praw.Reddit(client_id='7yjPl4FzmiQm1pbaTcF19Q', client_secret='3hcuOb_Eo8J_cbre7EECny9oq1l10A', user_agent='idek', check_for_async=False )
api = PushshiftAPI(reddit)

lcl = "./roastMe/"
write_path = "/kaggle/working/"
img_path = f"{lcl}images/"


def init_subs(start , end):
    submissions = api.search_submissions(after=start,
                                         before = end,
                                      subreddit='RoastMe',
                                      limit=18000)
    return submissions

data = set()
failed_imgs = []
ignore_path = 'RedditImageScraper/ignore_images/'
def get_img(url): 
    if "jpg" in submission.url.lower() or "png" in submission.url.lower():
        try:
            resp = requests.get(url.lower(), stream=True).raw
            image = np.asarray(bytearray(resp.read()), dtype="uint8")
            image = cv2.imdecode(image, cv2.IMREAD_COLOR)
            # Could do transforms on images like resize!
            compare_image = cv2.resize(image,(224,224))
            
            for (dirpath, dirnames, filenames) in os.walk(ignore_path):
                    ignore_paths = [os.path.join(dirpath, file) for file in filenames]
            ignore_flag = False
            
            for ignore in ignore_paths:
                    ignore = cv2.imread(ignore)
                    difference = cv2.subtract(ignore, compare_image)
                    b, g, r = cv2.split(difference)
                    total_difference = cv2.countNonZero(b) + cv2.countNonZero(g) + cv2.countNonZero(r)
                    if total_difference == 0:
                        ignore_flag = True
            if not ignore_flag:
                cv2.imwrite(f"{img_path}-{submission.id}.png", image)
                return True
            return False 
        except Exception as e  : 
            print(e, url)
            return False 



moff = 0
starting = pd.to_datetime('2015, 1,1')
with io.open(lcl + 'all_comments.csv', 'a+', encoding='utf-8') as file:
    while True:
        start_ts = (pd.to_datetime(starting)
                    + pd.DateOffset(months=moff)).to_pydatetime().timestamp()
        end_ts = (pd.to_datetime(starting) + pd.DateOffset(months=moff
                  + 1)).to_pydatetime().timestamp()
        submissions = init_subs(start_ts, end_ts)
        index = 0
        pdlen = 0

      # subreddit = reddit.subreddit('RoastMe')
        exceptionNumber = 0
        for submission in tqdm(submissions):
            if submission.id in data:
              if index % 100 == 0:
                  index += 1
              print (submission.id, 'alreADY there ')
              continue
            if not get_img(str(submission.url)): 
              failed_imgs.append([submission.id, submission.url])
              continue
        # submission.comments.replace_more(limit=0)

            temp = []
            try:
                for com in submission.comments:
                    if len(temp) > 20:
                        break
                    if not isinstance(com, MoreComments):
                        temp.append([
                            submission.id,
                            (str(com.body).replace('\n', '<newline>')).replace(',', '<coma>'),
                            len(com.body),
                            com.ups,
                            com.total_awards_received,
                            ])
            except KeyboardInterrupt as  k:
                print ('Interrupted')
                raise k
            except Exception as  e:
                exceptionNumber += 1
                print (submission.id, exceptionNumber, e)
                continue

            data.add(str(submission.id))
            file.write('<end_com>\n'.join([','.join([str(j) for j in
                       i]) for i in temp]))
            index += 1
            if index > pdlen + 15:
                pdlen = index
                with io.open(lcl + 'data.txt', 'wb') as pkl:
                    pickle.dump(data, pkl)

with io.open(lcl + 'data.txt', 'wb') as pkl:
    pickle.dump(data, pkl)
