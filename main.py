from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from src.recommend import Recommend
import requests
import os
from dotenv import load_dotenv, dotenv_values


load_dotenv() 

@asynccontextmanager
async def lifespan(app: FastAPI):
    rec = Recommend()
    app.state.rec = rec 
    yield  
    del app.state.rec

app = FastAPI(lifespan=lifespan)


origins = [
    "https://anirec-woad.vercel.app"
 ]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=['GET'],
    allow_headers=["*"],
)

@app.get('/')
async def root():
    return {"message": "Hello World"}

@app.get("/categories/anime/{anime_name}")
async def get_rec_anime(anime_name: str):
    return {'data': app.state.rec.get_rec_anime(anime_name)}

@app.get("/categories/genre/{genres}")
async def get_rec_genre(genres: str):
    genre_list = genres.split()
    return {'data': app.state.rec.get_rec_genre(genre_list)}

@app.get("/categories/user/{user}")
async def get_rec_user(user: str):
    url = "https://api.myanimelist.net/v2/users/{name}/animelist?&fields=id,title,genres,list_status&status=completed&limit=1000&field=list_status&sort=list_score".format(name = user)
    header = {"X-MAL-CLIENT-ID": os.getenv("X_MAL_CLIENT_ID")}

    r = requests.get(url, headers=header)
    r = r.json()
    completed_animes = r['data']
    while 'paging' in r and 'next' in r['paging']:
        r = requests.get(url=r['paging']['next'], headers=header).json()
        completed_animes.extend(r['data'])
    
    allAnimes = await get_all_animes_user(user)
    return {'data': app.state.rec.get_rec_user(completed_animes, allAnimes)}
  
async def get_all_animes_user(user: str):
    url = "https://api.myanimelist.net/v2/users/{name}/animelist?&fields=id,title,genres,list_status&limit=1000&field=list_status&sort=list_score".format(name = user)
    header = {"X-MAL-CLIENT-ID": os.getenv("X_MAL_CLIENT_ID")}

    r = requests.get(url, headers=header)
    r = r.json()
    animes = r['data']
    while 'paging' in r and 'next' in r['paging']:
        r = requests.get(url=r['paging']['next'], headers=header).json()
        animes.extend(r['data'])
    
    return animes

@app.get("/genres")
async def get_stats_anime():
    return {'data': app.state.rec.getAllGenres()}

