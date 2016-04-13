import process

df = process.TaqDataFrame('../data/my_random_dataset.zip','qts').load()
# print (df.df)
df.featurize('BLUFEN')