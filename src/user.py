import time
from typing import Tuple, List
import numpy as np
import pandas as pd
import psycopg2
import requests
import os
from dotenv import load_dotenv, dotenv_values

def getData(userlist, allAnimes):
        if(len(userlist) == 0): # no anime on their account.
            return -1
        
        animes: List[Tuple[int, str, int, str]] = []
        completed_animes: List[int] = [] ## using this for users that do not rate anime --> I only want to use anime they have completed.
        scores: List[int] = []

        for i in range(0, len(allAnimes)):
            ### getting name + score if rated
            name: str = allAnimes[i]['node']['title']
            id: int = allAnimes[i]['node']['id']
            score = -1
            if(allAnimes[i]['list_status']['score'] != 0):
                score = allAnimes[i]['list_status']['score']
                scores.append(score)
            
            ### counting genres
            genre: str = ''
            try:
              for j in range(0, len(allAnimes[i]['node']['genres'])):
                genre += allAnimes[i]['node']['genres'][j]['name'] + ','

            except KeyError:
                print(f'No genres were found for {name}')

            # cleaning up for tfidf algorithm

            genre = genre.replace(' ', '_')
            genre = genre.replace(',',' ')

            animes.append((id, name, score, genre))
            if(allAnimes[i]['list_status']['status'] == 'completed'):
                completed_animes.append(id)

        genres = getGenres(animes)

        topAnimes = []

        if(len(scores) != 0 and np.mean(scores) != scores[0]):
            z_scores: List[Tuple[int, str, float]] = []
            mean: float = np.mean(scores)
            std: float = np.std(scores) 
            positive_z_scores = np.array([], dtype=float) # 
            for i in range(0, len(scores)):
                z = (scores[i] - mean) / std
                if(z > 0):
                    positive_z_scores = np.append(positive_z_scores, z)
                id = animes[i][0]
                name = animes[i][1]
                z_scores.append((id, name, z))
            mean = positive_z_scores.mean()
            topAnimes = getTopAnimes(z_scores, mean)

            weights = getWeights(z_scores, mean)
            weights = weights[0:len(topAnimes)]
            print(f'top: {len(topAnimes)}')
            return {'topAnimes': topAnimes, 'genres': genres, 'weights': weights }
        
        else: ## case where user does not rate any animes or they rate everything the same --> using first 10 animes (or however much they have if below 10)
            for i in range(0, len(completed_animes)):
                topAnimes.append(completed_animes[i])
            topAnimes = getAnimes(topAnimes)
            if len(topAnimes) == 0:
                return -1

            return {'topAnimes': topAnimes, 'genres': genres }
                
def getWeights(z_scores, mean):
    '''
    returns the weight of each anime from topAnimes using its z_score minus the mean of all the positive z_scores.
    '''
    scores = [entry[2] for entry in z_scores]
    weights: list[float] = [score - mean for score in scores]

    return np.array(weights) 
        
        
def getGenres(animes):
    df = pd.DataFrame(animes, columns=['id', 'name', 'score', 'genres'])
    df = df.drop(columns=['name', 'score'], axis=1)

    return df

def getTopAnimes(z_scores, mean):
    config = os.getenv('PSYCOPG2_CRED')
    conn = psycopg2.connect(config)
    cur = conn.cursor()
    

    topAnimes: List[str] = []

        # keep adding until
        # 1. there have been 10 animes added
        
        # before 10 animes, stop if
        # z_score is below cutoff
    i = 0
    while((len(topAnimes) < 10) and (z_scores[i][2] > mean )): 
        sql = 'SELECT name FROM animes WHERE anime_id = %s'
        id = z_scores[i][0]
        print(f'trying {id}')
        cur.execute(sql, [id])
        query = cur.fetchone()

        if(query is not None): # has exact match in the anime table.
            print(f'exact match:  {query[0]}')
            addAnimes(topAnimes, id)
        else:
            sql = "WITH get_relations as (SELECT source_id, relation FROM relations, unnest(string_to_array(relations.relations, ', ', '')) as relation GROUP BY source_id, relation) SELECT anime_id FROM get_relations JOIN animes ON (CAST(get_relations.relation AS integer) = animes.anime_id) WHERE source_id = (SELECT source_id FROM get_relations WHERE CAST(relation AS integer) = %s)"
              # Checking the relations table to find the one that is in the actual anime table
            cur.execute(sql, [id])
            query = cur.fetchone()
            if(query is not None):
                print(f'already in relations table: {query[0]}')
                addAnimes(topAnimes, query[0])
            else:
                # Check relations to find the one that exists in the database.
                    # Find source material --> its adaptations = all relations of the anime.
                anime_id = updateRelations(id, cur, conn)
                if(anime_id is not None):
                    print(f'added, now in relations table: {anime_id}')
                    addAnimes(topAnimes, anime_id)
        i += 1
    return topAnimes

def getAnimes(animes):
    '''
    If no animes were rated. Does not consider z_scores.
    '''
    config = os.getenv('PSYCOPG2_CRED')
    conn = psycopg2.connect(config)
    cur = conn.cursor()

    topAnimes: List[str] = []
    i = 0
    while(len(topAnimes) < 10 and i < len(animes)):
        sql = 'SELECT name FROM animes WHERE anime_id = %s'
        id = animes[i]
        print(f'trying {id}')
        cur.execute(sql, [id])
        query = cur.fetchone()

        if(query is not None):
            print(f'exact match:  {query}')
            addAnimes(topAnimes, id)
        else:
            sql = "WITH get_relations as (SELECT source_id, relation FROM relations, unnest(string_to_array(relations.relations, ', ', '')) as relation GROUP BY source_id, relation) SELECT anime_id FROM get_relations JOIN animes ON (CAST(get_relations.relation AS integer) = animes.anime_id) WHERE source_id = (SELECT source_id FROM get_relations WHERE CAST(relation AS integer) = %s)"
              # Checking the relations table to see if already exists
            cur.execute(sql, [id])
            query = cur.fetchone()
            if(query is not None):
                print(f'already in relations table: {query[0]}')
                addAnimes(topAnimes, query[0])
            else:
                # Check relations to find the one that exists in the database.
                    # Find source material --> its adaptations = all relations of the anime.
                anime_id = updateRelations(id, cur, conn)
                if(anime_id is not None):
                    print(f'added, now in relations: {anime_id}')
                    addAnimes(topAnimes, anime_id)
        i += 1
    return topAnimes
    
def addAnimes(top, id):
    if(id not in top):
        top.append(id)

def updateRelations(id, cur, conn):
    check = "WITH get_relations as (SELECT source_id, relation FROM relations, unnest(string_to_array(relations.relations, ', ', '')) as relation GROUP BY source_id, relation) SELECT * FROM get_relations WHERE CAST(relation AS integer) = %s"
    sql = 'SELECT name FROM animes WHERE anime_id = %s'

    relations = getRelations(id) 
    
    matched = False
    anime_id = None
    source_id = None
    #
    for relation_id in relations['relations']: 
        exists = False
        if(not exists):
             cur.execute(check, [relation_id]) # check to see if a value already exists in the relations table. If so, append and update with new relations.
             q = cur.fetchone()
             if(q is not None):
                exists = True
                source_id = q[0]
                print(f'does exist... {source_id}')

        cur.execute(sql, [relation_id])
        query = cur.fetchone()
        if(query is not None):
            print(f'found in relations:  {query}')
            matched = True
            anime_id = relation_id
            break
    
    ## updating database

    if(matched):
        if(source_id is not None):
            getExisting = 'SELECT relations FROM relations WHERE source_id = %s'
            cur.execute(getExisting, [source_id])
            existing = cur.fetchone()
            new_relations = existing[0]
            added = ''
            for relation_id in relations['relations']:
                relation_id = str(relation_id)
                if(relation_id not in existing[0]):
                    new_relations += ', ' + relation_id
                    added += relation_id + ' '
            update_db = 'UPDATE relations SET relations = %s WHERE source_id = %s'

            data = (new_relations, source_id)
            cur.execute(update_db, (data))
            conn.commit()

            print(f'Updated {source_id}! Prev: {existing[0]} Added: {added}')

        else:
            relations_to_db = ', '.join(str(x) for x in relations['relations'])
            add_to_db = 'INSERT INTO relations (source_id, relations) VALUES (%s, %s)' 
            data = (relations['source_id'], relations_to_db)
            cur.execute(add_to_db, (data)) 
            conn.commit()
    
    return anime_id

    

def getRelations(id):
    '''
    Returns the source material id and the ids of animes that are related.
    '''
    source_id = getSource(id)
    if(source_id == -1): # anime original / no source found
        allRelations = []
        getRelation(id, allRelations)
        return {'source_id': -allRelations[0], 'relations': allRelations}

    url = 'https://api.jikan.moe/v4/manga/{id}/relations'.format(id=source_id)
    time.sleep(0.5)
    data = requests.get(url).json()['data']

    relations: List[int] = []

    for i in range(0, len(data)):
        if(data[i]['relation'] == 'Adaptation'):
            adapts = data[i]['entry']
            for entry in adapts:
                if(entry['type'] == 'anime'):
                    relations.append(entry['mal_id'])
            return {'source_id': source_id, 'relations': relations}

    

def getSource(id):
    '''
    Returns the source material id 
    '''
    url = 'https://api.jikan.moe/v4/anime/{id}/relations'.format(id=id)
    time.sleep(0.5)
    data = requests.get(url).json()['data']
    
    for i in range(0, len(data)):
        if(data[i]['relation'] == 'Adaptation'):
            ## can be multiple source materials - i.e. Classroom of the Elite has the LN (89357) and the newer manga (96371)
                # Look for the lowest id which ensures that it is the true source material (oldest)
            source = data[i]['entry']
            min = source[0]['mal_id']
            for entry in source:
                if(entry['mal_id'] < min):
                    min = entry['mal_id']
            return min
        
    # no adaptation --> anime original
    return -1 
    

        
            
def getRelation(id, recursedArr: list):
    blacklist = ['Other', 'Summary', 'Character']
    time.sleep(.5)
    recursedArr.append(id)

    try:
        response = requests.get("https://api.jikan.moe/v4/anime/{id}/relations".format(id = id))
        if response.status_code == 404:
            print('ignored 404')
            return 0
        response = response.json()
    except:
        print('uhhhh')
        return 0
    else:
        count = 0
        for relations in response['data']:
            if(relations['relation'] in blacklist): # blacklisting doodoo relations as they are often irrelevant
                continue
            for entries in relations['entry']:
                if(entries['mal_id'] not in recursedArr and entries['type'] == 'anime'):
                    return getRelation(entries['mal_id'], recursedArr)

    



                    








