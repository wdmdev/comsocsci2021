import numpy as np
import pandas as pd
import time
from threading import Thread
from psaw import PushshiftAPI

df = pd.read_csv('comsocsci2021/lectures/wallstreetbets.csv')

api = PushshiftAPI()

def download_comments(iter_id, ids):
    res = api.search_comments(
                            aggs='link_id', 
                            subreddit='wallstreetbets',
                            filter=['author', 'title', 'id', 'submission', 'score', 'created_utc', 'num_comments']
                        )
    
    df = pd.DataFrame(list(res))
    df.to_csv(f'wallstreetbets_comments{iter_id}.csv')

splits = 4
for split_id, ids in enumerate(np.array_split(df['id'], splits)):
    download_comments(split_id, ids)
    #Thread(target=lambda: download_comments(split_id, ids)).start()