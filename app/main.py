from fastapi import FastAPI
# from pathlib import Path
import pandas as pd

app = FastAPI()
# path_df = Path.cwd()


def get_url(url):
    file_id = url.split('/')[-2]
    dwn_url = 'https://drive.google.com/uc?id=' + file_id
    return dwn_url


hulu = pd.read_csv(get_url(
    "https://drive.google.com/file/d/10VVp6r3xUQdPElHyLTGGx1SystX6HNr-/view?usp=sharing"))
amazon = pd.read_csv(get_url(
    "https://drive.google.com/file/d/1JwVqKONNB5r_BatrfvCqhVmgOXfDMFXM/view?usp=sharing"))
disney = pd.read_csv(get_url(
    "https://drive.google.com/file/d/1-kgEsnWe5MilmUdxR0w9XZsKgv0CZqQy/view?usp=sharing"))
netflix = pd.read_csv(get_url(
    "https://drive.google.com/file/d/1AtMmOOhetbpo_MGhEU05QVT4rocyiSXR/view?usp=sharing"))

platforms = {"hulu": hulu, "amazon": amazon,
             "disney": disney, "netflix": netflix}


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
    # ratings
    uno = pd.read_csv(get_url(
        "https://drive.google.com/file/d/16Eo2OHIKFK131e_gzrPQmJnzbNy9kjZx/view?usp=sharing"))
    dos = pd.read_csv(get_url(
        "https://drive.google.com/file/d/1Ctp2AZH-e4uNckhOZ0C25rADgw07qO-h/view?usp=sharing"))
    tres = pd.read_csv(get_url(
        "https://drive.google.com/file/d/1Bgz6oHf3Pg4q2P2CDuJ8il4LYYBcUhyN/view?usp=sharing"))
    cuatro = pd.read_csv(get_url(
        "https://drive.google.com/file/d/1VLlMerf9VmIB_25aAVJ3eY6WB5oKsZF0/view?usp=sharing"))
    cinco = pd.read_csv(get_url(
        "https://drive.google.com/file/d/1Hw2oGjR97vlm_SxX9HI0v3ed8wl7j8KJ/view?usp=sharing"))
    seis = pd.read_csv(get_url(
        "https://drive.google.com/file/d/1_LpMkn3uV0otvWi0JyCwO67qJndDvmj4/view?usp=sharing"))
    siete = pd.read_csv(get_url(
        "https://drive.google.com/file/d/1s_dnmpK8j4n73aY-JxsPY2KEAOQhPpTl/view?usp=sharing"))
    ocho = pd.read_csv(get_url(
        "https://drive.google.com/file/d/1VDitO3dlGl5aXsRbMYQwjHJ7HGQO9P6p/view?usp=sharing"))

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
