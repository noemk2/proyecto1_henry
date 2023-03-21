from fastapi import FastAPI
from pathlib import Path
import pandas as pd

app = FastAPI()


hulu = pd.read_csv(Path.cwd() / "./app/dataset/hulu.csv")
amazon = pd.read_csv(Path.cwd() / "./app/dataset/amazon.csv")
disney = pd.read_csv(Path.cwd() / "./app/dataset/disney.csv")
netflix = pd.read_csv(Path.cwd() / "./app/dataset/netflix.csv")

platforms = {"hulu": hulu, "amazon": amazon,
             "disney": disney, "netflix": netflix}

# ratings
uno = pd.read_csv("./dataset/ratings/1.csv")
dos = pd.read_csv("./dataset/ratings/2.csv")
tres = pd.read_csv("./dataset/ratings/3.csv")
cuatro = pd.read_csv("./dataset/ratings/4.csv")
cinco = pd.read_csv("./dataset/ratings/5.csv")
seis = pd.read_csv("./dataset/ratings/6.csv")
siete = pd.read_csv("./dataset/ratings/7.csv")
ocho = pd.read_csv("./dataset/ratings/8.csv")


df = pd.concat([hulu, amazon, disney, netflix])


@app.get("/")
def read_root():
    return {hulu.keys()[0]}


@app.get("/get_max_duration/")
# Película con mayor duración con filtros opcionales de AÑO, PLATAFORMA Y TIPO DE DURACIÓN
# todo : el duration_type es irrelevante
async def get_max_duration(year: int = None, platform: str = None, duration_type: str = None):
    df_f = df[df["type"] == "movie"]
    df_f["duration_int"] = df_f["duration_int"].replace("g", 0)

    df_f["duration_int"] = df_f["duration_int"].astype(int)
    df_f["release_year"] = df_f["release_year"].astype(int)
    df_f["id"] = df_f["id"].astype(str)
    df_f["duration_type"] = df_f["duration_type"].astype(str)

    df_f = df_f.sort_values('duration_int', ascending=False)

    if year is not None:
        df_f = df_f.loc[df_f['release_year'] == year]
    if platform is not None:
        platform_arg = platform.lower()
        if platform_arg in platforms.keys():
            df_f = df_f.loc[df_f['id'].str[0] == platform_arg[0]]
        else:
            pass

    if duration_type is not None:
        df_f = df_f.loc[df_f['duration_type'] == duration_type]
    else:
        return df_f.iloc[0]["title"]

    return df_f.iloc[0]["title"]


# Cantidad de películas por plataforma con un puntaje mayor a XX en determinado año
@ app.get("/get_score_count/")
async def get_score_count(platform: str, scored: float, year: int):
    df_2 = pd.concat([uno, dos, tres, cuatro, cinco, seis, siete, ocho])
    score = df_2.groupby("movieId")["rating"].mean().reset_index()
    new_df = pd.DataFrame(
        {'id': score["movieId"], 'score': score["rating"]})

    select_platform = platforms[platform.lower()]
    platform_df = select_platform.loc[select_platform['release_year'] == year]
    merged_df = pd.merge(platform_df, new_df, on="id")
    merged_df = merged_df[merged_df['score'] > scored]
    return len(merged_df)


# Cantidad de películas por plataforma con filtro de PLATAFORMA.
@ app.get("/get_count_platform/")
async def get_count_platform(platform: str):
    select_platform = platforms[platform.lower()]
    return len(select_platform)


# Actor que más se repite según plataforma y año.
@ app.get("/get_actor/")
async def get_actor(platform: str, year: int):
    platform_arg = platform.lower()
    if platform_arg in platforms.keys():
        df_f = df.loc[df['id'].str[0] == platform_arg[0]]
    else:
        df_f = df

    df_f = df_f.loc[df_f['release_year'] == year]

    nombres_actores = df_f['cast'].str.split(',|:').explode()
    nombres_actores = nombres_actores.str.strip()
    conteo_actores = nombres_actores.value_counts()

    df_actores = pd.DataFrame(
        {'actor': conteo_actores.index, 'conteo': conteo_actores.values})

    df_actores = df_actores.drop(0).reset_index(drop=True)

    return df_actores["actor"][0]
