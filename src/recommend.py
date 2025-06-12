import numpy as np
from sklearn.neighbors import NearestNeighbors
from src.preprocess import DataFrames
from src.search import Search
import json
import pandas as pd

class Recommend(DataFrames):

    def __init__(self):
        super().__init__()
        self.search = Search(self.anime_df)

    def get_rec_anime(self, name):
        id = self.search.get_anime_id(name, 'name')
        
        if(id == -1): 
            return {'contains': self.search.sort_scores(self.search.get_similar_names(name, 'name')), 'fuzzy': self.search.get_fuzz_names(name)}

        s = self.item_similarity_df[id]
        s = s.sort_values(ascending=False).iloc[1:100] # top 100 (skipping itself)
        mapped_values = {}
        for i, values in s.items():
            score_weight = (self.anime_df[self.anime_df['anime_id'] == i].iloc[0,4] * .040)
            rating_sim = values * 1.2
            genre_sim = self.cosine_sim_df.loc[i, id]
            total = score_weight + rating_sim + genre_sim

            mapped_values[i] = total
        
        recced_animes = pd.Series(mapped_values).sort_values(ascending=False)
        rec_arr = []
        for i, values in recced_animes.items():
            anime = self.anime_df[self.anime_df['anime_id'] == int(i)]
            d = {
                 'id': int(anime.anime_id.iloc[0]),
                 'name': str(anime.name.iloc[0]), 
                 'image': str(anime.image.iloc[0]),
                 'english_name': str(anime.english_name.iloc[0]),
                 'other_name': str(anime.other_name.iloc[0]),
                 'synopsis': str(anime.synopsis.iloc[0]),
                 'genres': str(anime.genres.iloc[0]),
                 'score': float(anime.score.iloc[0])
                 }
            rec_arr.append(d)
        
        return rec_arr
    
    def get_rec_genre(self, genre_list: list):
        if('Sci-Fi' in genre_list):
            index = genre_list.index("Sci-Fi")
            genre_list[index] = "Sci_Fi"        
            
        filtered_animes = self.anime_df[self.anime_df['genres'].apply(lambda x: all(word in x for word in genre_list))]
        filtered_animes = filtered_animes.sort_values(by=['score'], ascending=False)
        filtered_animes = filtered_animes.drop('anime_id', axis = 1)
        filtered_animes = filtered_animes[filtered_animes['score'] >= 7.0].head(100)
        return json.loads(filtered_animes.to_json(orient="records"))
    
    def get_rec_user(self, userlist, allAnimes):
        data = self.getUserData(userlist, allAnimes)
        if(data == -1):
            return -1
        
        allAnimes = data['allAnimes']
        
        topAnimes = data['topAnimes']

        weights = data['weights']
    
        rating_similarity = self.get_rating_similarity_scores(topAnimes, allAnimes, weights)

        df = self.getDataFrame(rating_similarity.index.to_list())
        genre_similarity = self.get_genre_similarity_scores(animes=df)
        mapped_values = {}
        for i, values in rating_similarity.items():
            score_weight = (self.anime_df[self.anime_df['anime_id'] == i].iloc[0,4] * .040)
            rating_sim = values * 1.2
            genre_sim = genre_similarity.loc[i]
            total = score_weight + rating_sim + genre_sim

            mapped_values[i] = total
        
        recced_animes = pd.Series(mapped_values).sort_values(ascending=False)
        rec_arr = []
        for i, values in recced_animes.items():
            anime = self.anime_df[self.anime_df['anime_id'] == int(i)]
            d = {
                 'id': int(anime.anime_id.iloc[0]),
                 'name': str(anime.name.iloc[0]), 
                 'image': str(anime.image.iloc[0]),
                 'english_name': str(anime.english_name.iloc[0]),
                 'other_name': str(anime.other_name.iloc[0]),
                 'synopsis': str(anime.synopsis.iloc[0]),
                 'genres': str(anime.genres.iloc[0]),
                 'score': float(anime.score.iloc[0])
                 }
            rec_arr.append(d)
        
        return rec_arr
    
    
    def get_rating_similarity_scores(self, topAnimes, allAnimes, weights):
        '''
        use user-user similarity to determine similar anime to user's top animes
        '''
        similar= pd.Series([])
        for i, id in enumerate(topAnimes):
            s: pd.Series = self.item_similarity_df[id]
            s = s.sort_values(ascending=False)
            s = s.apply(lambda x: x + np.log(1 + weights[i]))
            similar = pd.concat([similar, s])

        similar = similar.sort_values(ascending=False)
        similar = similar[~similar.index.duplicated(keep='first')] # removes duplicate entries, keeping the highest similarity version
        similar = similar[~similar.index.isin(allAnimes)] # animes that have not been watched
    
        similar = similar.iloc[0:100]

        return similar
    
    def get_genre_similarity_scores(self, animes):
        '''
        use knn with the user's genre profile to find most similar to their genre preferences
        '''

        tfidf_matrix = self.getGenreTFIDF(animes)

        knn = NearestNeighbors(n_neighbors= tfidf_matrix.shape[0], metric='cosine') # Getting distance for every show in the matrix
        knn.fit(tfidf_matrix)


        distances, indices = knn.kneighbors(self.user_profile)
       
        genre_similarity = pd.Series(distances[0], index=animes.iloc[indices[0]].anime_id.values)
        genre_similarity = genre_similarity.apply(lambda x: 1 - x)

        return genre_similarity



        

    
        