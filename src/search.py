import pandas as pd
from thefuzz import fuzz

class Search():
    """
      Class for everything related to searching for an anime's name.

      --

      get_anime_id:
       exact search
       returns anime_id if exact match.

      get_fuzz_names:
        fuzzy search
        returns top 5 most similar names.
         i.e. 'Nruto' --> returns 'Naruto'.. etc.

      get_similar_names:
        contains search
        returns top 2 highest score names.
         i.e. 'Mushoku' --> returns 'Mushoku Tensei: Jobless Reincarnation'
    """
    def __init__(self, anime_df: pd.DataFrame):
        self.anime_df = anime_df

    def get_anime_id(self, name: str, titleCategory: str):
        """
        Queries input to (1) romaji name, (2) english name, (3) japanese name.
        Returns anime_id if exact match, else -1.
        """
    
        try:
            id = self.anime_df[self.anime_df[titleCategory] == name].anime_id.iloc[0]
            return id
        except IndexError: # no results found
            if(titleCategory == 'name'):
                return self.get_anime_id(name, 'english_name')
            elif(titleCategory == 'english_name'):
                return self.get_anime_id(name, 'other_name')
            else:
                return -1
    
    def create_dict(self, name, similarity):
        d = {'name': name, 'similarity': similarity}
        return d
    
    
    def get_fuzz_names(self, name):
        most_similar: list = [self.create_dict('', 0), self.create_dict('', 0), self.create_dict('', 0), self.create_dict('', 0), self.create_dict('', 0)]
        self.anime_df['name'].apply(lambda x: self.do_fuzz(x, name, most_similar))
        self.anime_df['english_name'].apply(lambda x: self.do_fuzz(x, name, most_similar))
        self.anime_df['other_name'].apply(lambda x: self.do_fuzz(x, name, most_similar))
        return list(filter(lambda x: x['similarity'] >= 70, most_similar)) ## filter
    
    def do_fuzz(self, row, name, most_similar: list):
        sim = fuzz.ratio(row, name)
        for i in range(0, 5):
            if(most_similar[i]['similarity'] < sim and self.create_dict(row, sim) not in most_similar):
                most_similar.insert(i, self.create_dict(row, sim))
                most_similar.pop()
                break

    def get_similar_names(self, name, type):
        dict = {} # doing dict to ensure only unique values are added by comparing keys
        a = self.anime_df[self.anime_df[type].fillna('').str.lower().str.contains(name.lower())]
  

        if(a is not None):
            a = a.sort_values(by=['score'], ascending=False).head(5)
            for i in range(0, len(a)):
                index = 1
                match(type):
                    case 'english_name':
                        index = 2
                    case 'other_name':
                        index = 3
                if(a.values[i][0] not in dict):
                    dict[a.values[i][0]] = (self.create_dict(a.values[i][index], a.values[i][4]))
        
        if(type == 'name'):
            dict.update(self.get_similar_names(name, 'english_name'))
        elif(type == 'english_name'):
            dict.update(self.get_similar_names(name, 'other_name'))
        return dict
    
    
    def sort_scores(self, d):
     arr = list(d.items())
     n = len(arr)

     if(n <= 1):
        return list(map(lambda x: x[1], arr))
     for i in range(1, n):
        key = arr[i]
        j = i - 1

        while(j >= 0 and key[1]['similarity'] > arr[j][1]['similarity']):
            arr[j + 1] = arr[j]
            j -= 1
        arr[j + 1] = key
     arr = list(map(lambda x: x[1], arr))
     return arr