import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
from typing import List
from src.user import getData
import os
from dotenv import load_dotenv, dotenv_values

class DataFrames:
    
    def __init__(self):
        load_dotenv()

        self.engine = create_engine(os.getenv("SQL_ALCHEMY_CRED"))
        self.anime_df = pd.read_sql("select * from animes", con=self.engine)

        # Scores only go from [-1, 10]
        self.anime_df['score'] = self.anime_df['score'].astype('float16')
        # Anime_id goes from [1, 55647]
        self.anime_df['anime_id'] = self.anime_df['anime_id'].astype('int32')

        self.anime_df['genres'] = self.anime_df['genres'].str.replace(',', '')
        self.anime_df['genres'] = self.anime_df['genres'].str.replace('-','_')
        self.anime_df['genres'] = self.anime_df['genres'].str.replace('(','')
        self.anime_df['genres'] = self.anime_df['genres'].str.replace(')', '')

        self.tfidf = TfidfVectorizer()
        tfidf_matrix = self.tfidf.fit_transform(self.anime_df['genres'])
        cosine_sim_genres = cosine_similarity(tfidf_matrix,tfidf_matrix)
        self.cosine_sim_df = pd.DataFrame(cosine_sim_genres, index = self.anime_df['anime_id'], columns = self.anime_df['anime_id'])

        self.item_similarity_df = pd.read_csv("Data.csv", index_col=0) ## Cosine similarities between each anime.
        print(self.item_similarity_df)
    
    def getUserData(self, userlist, allAnimes):
        '''
        gets user data, creates the user_profile as instance variable, returns topAnimes and allAnimes
        '''
        data = getData(userlist, allAnimes) # returns top animes + all animes and their genres
        if(data == -1):
            return -1
        
        df = data['genres']
        tfidf_matrix = self.getGenreTFIDF(df)
    
        user_profile = tfidf_matrix.mean(axis=0)
        self.user_profile = np.asarray(user_profile)

        allAnimes: set[int] = set(df['id'])

        topAnimes = data['topAnimes']

        weights = None
        if('weights' not in data):
            weights = np.zeros(len(topAnimes))
        else:
            weights = data['weights']

        return {'topAnimes': topAnimes, 'allAnimes': allAnimes, 'weights': weights}
    
    def getDataFrame(self, animes: List[int]):
        '''
        Converts a list of series with anime_ids into a dataframe containing all info of those animes.
        '''
        ids = animes

        df = self.anime_df[self.anime_df['anime_id'].isin(ids)]
        return df
    
    def getGenreTFIDF(self, df: pd.DataFrame):
        '''
        returns sparse matrix of tfidf on genres
        '''
        return self.tfidf.transform(df['genres'])
    
    def getAllGenres(self):
        '''
        For the front end to get all possible genres
        '''
        genres_df = pd.read_sql("select genre FROM genre_counts", con=self.engine)
        genres = genres_df['genre'].tolist()
        return genres
    
    



    






    
        

    
   

