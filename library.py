# Making necesarry imports
import sys
import os
import re
import pandas as pd
import sklearn.metrics as metrics
import numpy as np
from sklearn.neighbors import NearestNeighbors
from sklearn.metrics.pairwise import pairwise_distances


metric = 'cosine'
k = 3


def recommendItem(userId, ratings, metric=metric, recommendtype=2):
    if (userId not in ratings.index.values) or type(userId) is not int:
        print("User id should be a valid integer from this list :")
    else:
        prediction = []

        for i in range(ratings.shape[1]):
            # if (ratings[ratings.columns[i]][userId] != 0):  # not rated already
            prediction.append(predict_userbased(
                userId, ratings.columns[i], ratings, metric))
            # else:
            # prediction.append(-1)  # for already rated items
        prediction = pd.Series(prediction)
        prediction = prediction.sort_values(ascending=False)
        # print(prediction)
        recommended = prediction[:3]

        recommended_books=','.join(str(item) for item in set(recommended.to_list()))
        
        return recommended_books
        

def predict_userbased(user_id, item_id, ratings, metric=metric, k=k):
    prediction = 0
    user_loc = ratings.index.get_loc(user_id)
    item_loc = ratings.columns.get_loc(item_id)
    # similar users based on cosine similarity
    similarities, indices = findksimilarusers(user_id, ratings, metric, k)
    # to adjust for zero based indexing
    mean_rating = ratings.iloc[user_loc, :].mean()
    sum_wt = np.sum(similarities)-1
    product = 1
    wtd_sum = 0

    for i in range(0, len(indices.flatten())):
        if indices.flatten()[i] == user_loc:
            continue
        else:
            ratings_diff = ratings.iloc[indices.flatten(
            )[i], item_loc]-np.mean(ratings.iloc[indices.flatten()[i], :])
            product = ratings_diff * (similarities[i])
            wtd_sum = wtd_sum + product

    # in case of very sparse datasets, using correlation metric for collaborative based approach may give negative ratings
    # which are handled here as below
    if sum_wt == 0.0:
        sum_wt = 0.1

    prediction = int(round(mean_rating + (wtd_sum/sum_wt)))
    if prediction <= 0:
        prediction = 1
    elif prediction > 10:
        prediction = 10

    return prediction


def findksimilarusers(user_id, ratings, metric=metric, k=k):
    similarities = []
    indices = []
    model_knn = NearestNeighbors(metric=metric, algorithm='brute')
    model_knn.fit(ratings)
    loc = ratings.index.get_loc(user_id)
    distances, indices = model_knn.kneighbors(
        ratings.iloc[loc, :].values.reshape(1, -1), n_neighbors=k+1)
    similarities = 1-distances.flatten()

    return similarities, indices


def recommendations(userid, ratings_matrix):
    return recommendItem(userid, ratings_matrix, 'cosine', 2)
