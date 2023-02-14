import praw
import pandas as pd

clientId = '9TBprrrlrdt1DEdFgjdR1w'
secretKey = 'zKqmzHgvVkqjlqvIk5K_JQyszuKNpg'
userID = 'AltruisticWish7'
userAgent = 'Creating ETL pipeline using api /u/AltruisticWish7'

reddit = praw.Reddit(
    
    client_id = clientId,
    client_secret = secretKey,
    user_agent = userAgent,
    username = userID
)

subreddits = ['UBreddit','statistics','3Blue1Brown']

fields_to_capture1 = [ 'created_utc', 
                     'is_crosspostable', 'is_self', 'is_video', 'locked', 'media_only', 'over_18',
                     'subreddit_id', 'subreddit_name_prefixed', 'subreddit_subscribers', 
                     'title', 'permalink', 
                     'total_awards_received', 'downs','gilded','num_comments', 'num_crossposts', 'num_reports', 
                     'ups', 'author_name']

l = 1000
scrap_data = []
for sub in subreddits:
    for submission in reddit.subreddit(sub).top(limit=l):
        row  = []

        row.append(submission.created_utc)
        row.append(submission.is_crosspostable)
        row.append(submission.is_self)
        row.append(submission.is_video)
        row.append(submission.locked)
        row.append(submission.media_only)
        row.append(submission.over_18)
        row.append(submission.subreddit_id)
        row.append(submission.subreddit_name_prefixed)
        row.append(submission.subreddit_subscribers)
        row.append(submission.title)
        row.append(submission.permalink)
        row.append(submission.total_awards_received)
        row.append(submission.downs)
        row.append(submission.gilded)
        row.append(submission.num_comments)
        row.append(submission.num_crossposts)
        row.append(submission.num_reports)
        row.append(submission.ups)
        row.append(submission.author)
            
        scrap_data.append(row)

df = pd.DataFrame(data = scrap_data, columns = fields_to_capture1)

df.to_csv(r'/Users/vaibhavivichare/Documents/Other/ETL/API_to_Postgres/reddit_data.csv', sep= '\t', index = False)

