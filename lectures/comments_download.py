import os
import time
import numpy as np
from threading import Thread
from threading import Lock
import datetime as dt
from psaw import PushshiftAPI
import pandas as pd
from tqdm import tqdm

def download_comments(api, bar, link_ids, id):
    start=int(dt.datetime(2020,1,1).timestamp())
    end=int(dt.datetime(2021,1,25).timestamp())
    comments = []
    N = 10
    step = int(len(link_ids)/N) #split into N calls
    for i in range(N):
        ids = link_ids[int(round(i*step)):int(round((i+1)*step))]
        comment_results = list(api.search_comments(
                                    after=start,
                                    before=end,
                                    subreddit='wallstreetbets',
                                    link_id=ids,
                                    filter=['id', 'score', 'created_utc', 'author', 'parent_id']))
        comments.extend(comment_results)
        bar.update(len(ids))

    comments_df = pd.DataFrame(comments)
    comments_df.drop('d_', axis=1, inplace=True)
    comments_df.to_csv('wallstreetbets_comments_{id}.csv', index=False)

if __name__ == '__main__':
    api = PushshiftAPI()
    submissions = pd.read_csv(os.path.join('lectures', 'wallstreetbets.csv'))
    #threads = []
    link_ids = np.array_split(submissions[submissions['num_comments'] > 0]['id'], 4)
    bar1 = tqdm(total=len(link_ids[0]), position=0)
    bar2 = tqdm(total=len(link_ids[1]), position=1)
    bar3 = tqdm(total=len(link_ids[2]), position=2)
    bar4 = tqdm(total=len(link_ids[3]), position=3)
    #threads.append(Thread(target=lambda: download_comments(api, bar1, link_ids[0], 1)))
    #threads.append(Thread(target=lambda: download_comments(api, bar2, link_ids[1], 2)))
    #threads.append(Thread(target=lambda: download_comments(api, bar3, link_ids[2], 3)))
    #threads.append(Thread(target=lambda: download_comments(api, bar4, link_ids[3], 4)))

    ## start download threads
    #for t in threads:
    #    t.start()

    ## wait for download threads to finish
    #for t in threads:
    #    t.join()
    
    download_comments(api, bar1, link_ids[0], 1)
    download_comments(api, bar2, link_ids[1], 2)
    download_comments(api, bar3, link_ids[2], 3)
    download_comments(api, bar4, link_ids[3], 4)

    print('Download completed')
