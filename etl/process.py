from sys import argv

for line in open(argv[1]):
    tweet_id, hashtags, hashtags_count, user_id, created_at, followers_count, score, text = line.replace('\n', '').split('\t')
    print ','.join([tweet_id, user_id, created_at, followers_count, score, text])
