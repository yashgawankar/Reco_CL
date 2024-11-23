import pandas as pd
df = pd.read_csv('ml-100k/u.item', sep='\t', names=['user_id', 'item_id', 'rating', 'timestamp'])

# Group by user_id and count the number of ratings
user_ratings_count = df.groupby('user_id')['rating'].count()

# Filter out users with only one rating
filtered_users = user_ratings_count[user_ratings_count > 5].index

# Filter the original DataFrame to include only the filtered users
filtered_df = df[df['user_id'].isin(filtered_users)]

# Group by item_id and count the number of ratings
movie_ratings_count = filtered_df.groupby('item_id')['rating'].count()

# Filter out movies with less than 5 ratings
filtered_movies = movie_ratings_count[movie_ratings_count >= 5].index

# Filter the DataFrame to include only the filtered movies
filtered_df = filtered_df[filtered_df['item_id'].isin(filtered_movies)]

# Find total number of items rated 4 or higher by user in filtered df and plot a histogram with customized bins (taking a list as input)

# Find total number of items rated 4 or higher by user in filtered df
high_rated_items_count = filtered_df[filtered_df['rating'] >= 4].groupby('user_id')['item_id'].count()

# filter all interactions from filtered_df such that the ratings are 4 or higher.

# Filter interactions with ratings of 4 or higher
high_rated_interactions = filtered_df[filtered_df['rating'] >= 4]

# Print the size of the filtered DataFrame
print("Size of DataFrame with high ratings:", high_rated_interactions.size)

# convert the timestamp to a readable format - dd/mm/yyyy

from datetime import datetime

# Assuming 'timestamp' column contains Unix timestamps
high_rated_interactions['readable_timestamp'] = high_rated_interactions['timestamp'].apply(lambda x: datetime.fromtimestamp(x).strftime('%d/%m/%Y'))

# Display the DataFrame with the new column
print(high_rated_interactions.head())

# Convert timestamp to datetime objects
high_rated_interactions['datetime'] = pd.to_datetime(high_rated_interactions['timestamp'], unit='s')

# Extract year and month
high_rated_interactions['year_month'] = high_rated_interactions['datetime'].dt.to_period('M')

# Group by user_id and year_month, count interactions
user_monthly_interactions = high_rated_interactions.groupby(['user_id', 'year_month'])['item_id'].count()

# Filter out user-month combinations with less than 5 interactions
filtered_user_monthly_interactions = user_monthly_interactions[user_monthly_interactions >= 5]

# Get the indices (user_id, year_month) of the filtered interactions
filtered_indices = filtered_user_monthly_interactions.index

# Filter the original high_rated_interactions DataFrame based on the filtered indices
filtered_high_rated_interactions = high_rated_interactions.set_index(['user_id', 'year_month']).loc[filtered_indices].reset_index()

# Now filtered_high_rated_interactions contains only interactions where each user has at least 5 interactions every month
print("Size of DataFrame with high ratings and at least 5 interactions per user per month:", filtered_high_rated_interactions.size)
filtered_high_rated_interactions.head()


# Assign new Ids to the items to make them consecutive and maintain the map

# Create a mapping from old item IDs to new consecutive IDs
old_to_new_item_id = {}
new_item_id = 1

# Iterate through unique item IDs in the filtered DataFrame
for item_id in filtered_high_rated_interactions['item_id'].unique():
  if item_id not in old_to_new_item_id:
    old_to_new_item_id[item_id] = new_item_id
    new_item_id += 1

# Create a new column with the new consecutive item IDs
filtered_high_rated_interactions['new_item_id'] = filtered_high_rated_interactions['item_id'].map(old_to_new_item_id)

# Now you have a DataFrame with both the old and new item IDs, and the mapping is stored in old_to_new_item_id
print(filtered_high_rated_interactions.head())

# Count total no of unique items in filtered_high_rated_interactions

# Count the number of unique items in filtered_high_rated_interactions
num_unique_items = filtered_high_rated_interactions['item_id'].nunique()

print("Total number of unique items in filtered_high_rated_interactions:", num_unique_items)

# Split each interaction from filtered_high_rated_interactions monthwise and store each of the interactions in a dictionary where the key is "period_0" for 1st month and so on in order, and the value is a list of tuple - (user_id, new_item_id)

monthly_interactions = {}
unique_months = sorted(filtered_high_rated_interactions['year_month'].unique())
for i, month in enumerate(unique_months):
  month_data = filtered_high_rated_interactions[filtered_high_rated_interactions['year_month'] == month]
  interactions = [(row['user_id'], row['new_item_id']) for _, row in month_data.iterrows()]
  monthly_interactions[f"period_{i}"] = interactions


# print the key and size of the value for each k,v pair

for key, value in monthly_interactions.items():
  print(f"Key: {key}, Size of value: {len(value)}")


# Save each of the values in the dict in a text file that corresponds to its key, labeled as <key>.txt. In each text file, store the corresponding values such that each ele of the tuple is separated by a space and each tuple is on a new line.
# Store this in ADER/data/MOVIELENS100K/ directory here

import os

# Create the directory if it doesn't exist
directory = "ADER/data/MOVIELENS100K/"
if not os.path.exists(directory):
    os.makedirs(directory)

for key, value in monthly_interactions.items():
  file_path = os.path.join(directory, f"{key}.txt")
  with open(file_path, "w") as f:
    for user_id, item_id in value:
      f.write(f"{user_id} {item_id}\n")

