import pandas as pd


def query_tweets(term, df):
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
   
    term_filter = df['text'].str.contains(term, case=False, na=False)
    filtered_df = df[term_filter]
    
  
    if pd.api.types.is_datetime64_any_dtype(filtered_df['created_at']):
               tweet_counts_by_day = filtered_df.groupby(filtered_df['created_at'].dt.date)['text'].count()
       
        unique_users = filtered_df['author_id'].nunique()
       
        avg_likes = filtered_df['like_count'].mean()
        
        
        place_ids = filtered_df['place_id'].unique()
       
        tweet_times = filtered_df['created_at'].dt.hour.value_counts().sort_index()
        
               top_user = filtered_df['author_handle'].value_counts().idxmax()
        
        
        return {
            'tweet_counts_by_day': tweet_counts_by_day,
            'unique_users': unique_users,
            'avg_likes': avg_likes,
            'place_ids': place_ids,
            'tweet_times': tweet_times,
            'top_user': top_user
        }
    else:
        print("Datetime conversion failed for 'created_at'. Please check the column format.")
        return None




file_path = 'C:/Users/ASUS USER/OneDrive/Desktop/python/data_anal/correct_twitter_201904.tsv'
df = pd.read_csv(file_path, sep='\t', encoding='utf-8')




term = str(input("Input the term to search \n"))
results = query_tweets(term, df)

# Print the results
print(f"Tweet counts by day:\n{results['tweet_counts_by_day']}")
print(f"Unique users who tweeted about {term}: {results['unique_users']}")
print(f"Average likes on tweets containing {term}: {results['avg_likes']}")
print(f"Place IDs where tweets originated: {results['place_ids']}")
print(f"Tweet posting times (hour of the day):\n{results['tweet_times']}")
print(f"User who posted the most tweets containing {term}: {results['top_user']}")
