### Warnings ###
import warnings

warnings.filterwarnings("ignore")

### System ###
import os
import re
import asyncio
import datetime as dt
from collections import defaultdict
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

### Console ###
from tqdm.rich import tqdm
from rich.console import Console

### Typing ###
from typing import *
from pydantic import BaseModel, BaseSettings

### Connectivity ###
import aiohttp
import requests

### Parsing ###
import arrow
from bs4 import BeautifulSoup

### Database ###
from psycopg2.extras import execute_values
from pony.orm import (
    Database,
    PrimaryKey,
    Required,
    db_session,
    # select,
    # count,
    commit,
    Optional as dOptional,
    Set as dSet,
)


class Settings(BaseSettings):
    DB_TYPE: str = "sqlite"
    DB_FILE: str = "db.sqlite"
    DATABASE_URL: str = None
    BASE_URL: str = "https://tfgames.site"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


config = Settings()

console = Console()

db = Database()


class Game(db.Entity):
    id = PrimaryKey(int, auto=True)
    title = Required(str)
    engine = Required("GameEngine")
    content_rating = Required("ContentRating")
    language = Required(str)
    release_date = Required(dt.datetime)
    last_update = Required(dt.datetime)
    version = Required(str)
    development_stage = Required(str)
    likes = Required(int)
    contest = dOptional(str)
    orig_pc_gender = dOptional(str)
    adult_themes = dSet("AdultTheme")
    transformation_themes = dSet("TransformationTheme")
    multimedia_themes = dSet("MultimediaTheme")
    thread = dOptional(str)
    play_online = dOptional(str)
    synopsis_text = dOptional(str)
    synopsis_html = dOptional(str)
    plot_text = dOptional(str)
    plot_html = dOptional(str)
    characters_text = dOptional(str)
    characters_html = dOptional(str)
    walkthrough_text = dOptional(str)
    walkthrough_html = dOptional(str)
    changelog_text = dOptional(str)
    changelog_html = dOptional(str)
    authors = dSet("GameAuthor")
    versions = dSet("GameVersion")
    reviews = dSet("Review")


class GameEngine(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    games = dSet(Game)


class ContentRating(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    games = dSet(Game)


class AdultTheme(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    games = dSet(Game)


class TransformationTheme(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    games = dSet(Game)


class MultimediaTheme(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    games = dSet(Game)


class GameAuthor(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    games = dSet(Game)


class GameDownload(db.Entity):
    id = PrimaryKey(int, auto=True)
    link = Required(str)
    report = Required(str)
    note = dOptional(str, nullable=True)
    delete = dOptional(str, nullable=True)
    game_version = Required("GameVersion")


class GameVersion(db.Entity):
    id = PrimaryKey(int, auto=True)
    version = Required(str)
    downloads = dSet(GameDownload)
    game = Required(Game)


class Review(db.Entity):
    id = PrimaryKey(int, auto=True)
    author = Required(str)
    text = Required(str)
    date = Required(dt.datetime)
    version = Required(str)
    game = Required(Game)


if config.DB_TYPE == "sqlite":
    db.bind(provider="sqlite", filename=os.path.abspath(config.DB_FILE), create_db=True)
elif config.DB_TYPE == "postgres":
    parsed = urlparse(config.DATABASE_URL)
    db.bind(
        provider="postgres",
        user=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port,
        database=parsed.path.lstrip("/"),
    )
elif config.DB_TYPE == "mysql":
    parsed = urlparse(config.DATABASE_URL)
    db.bind(
        provider="mysql",
        user=parsed.username,
        passwd=parsed.password,
        host=parsed.hostname,
        port=parsed.port,
        db=parsed.path.lstrip("/"),
    )


db.generate_mapping(create_tables=True)


class PReview(BaseModel):
    id: int
    author: str
    version: str
    date: dt.datetime
    text: str


class PGame(BaseModel):
    id: int
    title: str
    authors: Dict[str, int]
    game_engine: str
    content_rating: str
    language: str
    release_date: dt.datetime
    last_update: dt.datetime
    version: str
    development_stage: str
    likes: int
    reviews: Optional[List[PReview]]
    contest: Optional[str]
    orig_pc_gender: Optional[str]
    themes: Optional[dict]
    thread: Optional[str]
    play_online: Optional[str]
    versions: Optional[dict]
    synopsis: Optional[Dict[str, str]]
    plot: Optional[Dict[str, str]]
    characters: Optional[Dict[str, str]]
    walkthrough: Optional[Dict[str, str]]
    changelog: Optional[Dict[str, str]]


class PGameReduced(BaseModel):
    id: int
    title: str
    authors: Dict[str, int]
    game_engine: str
    content_rating: str
    language: str
    release_date: dt.datetime
    last_update: dt.datetime
    version: str
    development_stage: str
    likes: int
    contest: Optional[str]
    orig_pc_gender: Optional[str]
    themes: Optional[dict]
    thread: Optional[str]
    play_online: Optional[str]
    versions: Optional[dict]
    synopsis: Optional[Dict[str, str]]
    plot: Optional[Dict[str, str]]
    characters: Optional[Dict[str, str]]
    walkthrough: Optional[Dict[str, str]]
    changelog: Optional[Dict[str, str]]


class PGameSearchResult(BaseModel):
    id: int
    title: str
    authors: Dict[str, int]
    game_engine: str
    content_rating: str
    language: str
    release_date: dt.datetime
    last_update: dt.datetime
    version: str
    development_stage: str
    likes: int
    contest: Optional[str]
    orig_pc_gender: Optional[str]
    themes: Optional[dict]
    thread: Optional[str]
    play_online: Optional[str]


def pgame_to_db_game(cur, game):
    # authors_to_add = [(v, k) for k, v in game.authors.items() if not GameAuthor.get(name=k, id=v)]
    # execute_values(cur, f"insert into gameauthor (id, name) values %s", authors_to_add)

    for k, v in game.authors.items():
        author = GameAuthor.get(name=k, id=v)
        if not author:
            GameAuthor(id=v, name=k)

    db_game = Game(
        id=game.id,
        title=game.title,
        authors=[GameAuthor.get(id=v) for v in game.authors.values()],
        version=game.version or "1.0.0",
        engine=GameEngine.get(name=game.game_engine),
        content_rating=ContentRating.get(name=game.content_rating),
        language=game.language,
        release_date=game.release_date,
        last_update=game.last_update,
        development_stage=game.development_stage,
        likes=game.likes,
        contest=game.contest or "",
        orig_pc_gender=game.orig_pc_gender,
        adult_themes=[AdultTheme.get(id=v) for v in game.themes["adult"].values()]
        if game.themes
        else [],
        transformation_themes=[
            TransformationTheme.get(id=v)
            for v in game.themes["transformation"].values()
        ]
        if game.themes
        else [],
        multimedia_themes=[
            MultimediaTheme.get(id=v) for v in game.themes["multimedia"].values()
        ]
        if game.themes
        else [],
        thread=game.thread or "",
        play_online=game.play_online or "",
        synopsis_text=game.synopsis["text"] if game.synopsis else "",
        synopsis_html=game.synopsis["html"] if game.synopsis else "",
        plot_text=game.plot["text"] if game.plot else "",
        plot_html=game.plot["html"] if game.plot else "",
        characters_text=game.characters["text"] if game.characters else "",
        characters_html=game.characters["html"] if game.characters else "",
        walkthrough_text=game.walkthrough["text"] if game.walkthrough else "",
        walkthrough_html=game.walkthrough["html"] if game.walkthrough else "",
        changelog_text=game.changelog["text"] if game.changelog else "",
        changelog_html=game.changelog["html"] if game.changelog else "",
    )

    # commit()

    db_versions = []
    for version, downloads in game.versions.items():
        db_downloads = []

        version = GameVersion(version=version, game=db_game)
        for download in downloads:
            db_downloads.append(
                GameDownload(
                    delete=download["delete"] or "",
                    link=download["link"],
                    note=download["note"] or "",
                    report=download["report"],
                    game_version=version,
                )
            )

        # downloads_to_add = [(d["link"], d["report"], d["note"], d["delete"], version.to_dict()["id"]) for d in downloads]
        # execute_values(cur, f"insert into gamedownload (link, report, note, delete, game_version) values %s", downloads_to_add)

        # db_downloads = list((d for d in GameDownload.select() if d.game_version == version))

        # print(db_downloads)

        version.downloads = db_downloads
        db_versions.append(version)

    db_game.versions = db_versions

    reviews = []
    for review in game.reviews:
        reviews.append(
            Review(
                author=review.author,
                text=review.text,
                date=review.date,
                version=review.version,
                game=db_game,
            )
        )
    # reviews_to_add = [(r.author, r.text, r.date, r.version, db_game.id) for r in game.reviews]
    # execute_values(cur, f"insert into review (author, text, date, version, game) values %s", reviews_to_add)

    db_game.reviews = reviews

    return db_game


def parse_game_page(game_id, html_game, html_reviews):
    item = BeautifulSoup(html_game, features="lxml")
    reviews_soup = BeautifulSoup(html_reviews, features="lxml")

    data = defaultdict(lambda: defaultdict(dict))
    data["authors"] = {}
    data["versions"] = defaultdict(list)
    data["play_online"] = None
    data["likes"] = 0
    data["reviews"] = []

    data["title"] = item.find(class_="viewgamecontenttitle").text.strip()

    # Game

    container = item.find(class_="viewgamecontentauthor")
    links = container.find_all("a")
    if links:
        for link in links:
            try:
                data["authors"][link.text.lower().replace(" ", "_")] = int(
                    link.get("href").split("u=")[1]
                )
            except:
                continue
    else:
        try:
            author = (
                container.text.strip().lstrip("by").strip().lower().replace(" ", "_")
            )
            data["authors"][author] = GameAuthor.get(name=author).id
        except:
            return

    game_info = item.select(".viewgamesidecontainer > .viewgameanothercontainer")[0]

    for box in game_info.find_all(class_="viewgameinfo"):
        left = box.find(class_="viewgameitemleft").text
        right = box.find(class_="viewgameitemright")

        if left == "Engine":
            data["game_engine"] = right.text.lower().replace(" ", "_")
        elif left == "Rating":
            data["content_rating"] = right.text.lower().replace(" ", "_")
        elif left == "Language":
            data["language"] = right.text
        elif left == "Release Date":
            result = right.text
            try:
                data["release_date"] = arrow.get(
                    result, "|DD MMM YYYY|, HH:mm"
                ).datetime
            except:
                pass
            try:
                data["release_date"] = arrow.get(result, "MM/DD/YYYY").datetime
            except:
                pass
        elif left == "Last Update":
            result = right.text
            try:
                data["last_update"] = arrow.get(result, "|DD MMM YYYY|, HH:mm").datetime
            except:
                pass
            try:
                data["last_update"] = arrow.get(result, "MM/DD/YYYY").datetime
            except:
                pass
        elif left == "Version":
            data["version"] = right.text
        elif left == "Development":
            data["development_stage"] = right.text
        elif left == "Likes":
            data["likes"] = int(right.text)
        elif left == "Contest":
            result = right.text
            data["contest"] = None if result == "None" else result
        elif left == "Orig PC Gender":
            data["orig_pc_gender"] = right.text
        elif left == "Adult Themes":
            for link in right.find_all("a"):
                data["themes"]["adult"][link.text] = int(
                    link.get("href").split("adult=")[1]
                )
        elif left == "TF Themes":
            for link in right.find_all("a"):
                data["themes"]["transformation"][link.text] = int(
                    link.get("href").split("transformation=")[1]
                )
        elif left == "Multimedia":
            for link in right.find_all("a"):
                data["themes"]["multimedia"][link.text] = int(
                    link.get("href").split("multimedia=")[1]
                )
        elif left == "Discussion/Help":
            data["thread"] = right.find("a").get("href")

    downloads = item.find(id="downloads")

    for container in downloads:
        if container.name == "center":
            version = container.text.lstrip("Version:").strip()
        elif container.name == "div":
            link = {}
            link["delete"] = container.find(class_="dldeadlink").find("a")
            link["link"] = container.find(class_="dltext").find("a").get("href")
            try:
                link["note"] = container.find(class_="dlnotes").find("img").get("title")
            except:
                link["note"] = None
            link["report"] = urljoin(
                config.BASE_URL,
                container.find(class_="dlreportdeadlink").find("a").get("href"),
            )
            data["versions"][version or data["version"]].append(link)

    for i in range(1, 6):
        tab = item.find(id=f"tabs-{i}")
        if not tab:
            continue
        title = item.find("a", {"href": f"#tabs-{i}"}).text
        data[title.lower()] = {}
        data[title.lower()]["text"] = tab.text
        data[title.lower()]["html"] = str(tab)

    play_online = item.find(id="play")
    if play_online:
        data["play_online"] = play_online.find("form").get("action")

    # Reviews

    for i, review in enumerate(reversed(reviews_soup.find_all(class_="reviewcontent"))):
        lines = [line for line in review.text.split("\n") if line.strip()]
        if "Review by" not in lines[0]:
            continue
        author = lines[0].lstrip("Review by").strip()
        m = re.match(r"Version reviewed: (.+) on (.*)", lines[1])
        if not m:
            continue
        version, date = m.groups()
        try:
            date = arrow.get(date, "YYYY-MM-DD HH:mm:ss")
        except:
            date = arrow.get(date, "MM/DD/YYYY HH:mm:ss")
        text = "\n".join(lines[2:])
        if not text:
            continue
        data["reviews"].append(
            PReview(
                id=i,
                author=author,
                version=version,
                date=date.datetime,
                text=text,
            )
        )

    return PGame(id=game_id, **data)


def parse_category(html, name):
    soup = BeautifulSoup(html, "lxml")

    objects = []
    for item in soup.find_all("div", class_="browsecontainer"):
        data = item.find("a")
        _name = data.text
        _link = f'"https://tfgames.site/{data["href"]}'
        _id = int(_link.split(f"{name}=")[1])
        _repr = _name.lower().replace(" ", "_")
        objects.append((_id, _repr))

    return objects


def fetch_page_raw(game_id, _type, url):
    r = requests.get(url)
    return game_id, _type, r.text


@db_session
def crawl():
    con = db.get_connection()
    cur = con.cursor()

    console.log("Fetching Categories")
    cat_data = [
        ("engine", "gameengine", GameEngine),
        ("rating", "contentrating", ContentRating),
        ("adult", "adulttheme", AdultTheme),
        ("transformation", "transformationtheme", TransformationTheme),
        ("multimedia", "multimediatheme", MultimediaTheme),
        ("author", "gameauthor", GameAuthor),
    ]

    for name, table_name, db_cls in tqdm(cat_data, unit="categories"):
        r = requests.get(f"https://tfgames.site/?module=browse&by={name}")
        data = parse_category(r.text, name)
        for _id, _repr in data:
            db_cls(id=_id, name=_repr)
        # execute_values(cur, f"insert into {table_name} (id, name) values %s", data)

    with console.status("Fetching list of games..."):
        payload = "module=search&search=1&likesmin=0&likesmax=0&development%5B%5D=11&development%5B%5D=12&development%5B%5D=18&development%5B%5D=41&development%5B%5D=46&development%5B%5D=47"

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        r = requests.post(
            "https://tfgames.site/index.php", data=payload, headers=headers
        )

        soup = BeautifulSoup(r.text, features="lxml")

        table = soup.find("table")

        game_links = []
        for row in table.find_all("tr"):
            cols = row.find_all("td")

            if not cols:
                continue

            game_links.append(
                urljoin(config.BASE_URL, f'/{cols[0].find("a").get("href")}')
            )

    console.log("Fetching game info")
    all_links = []
    for url in sorted(game_links):
        game_id = int(url.split("id=")[1])
        all_links.append((game_id, "game", url))
        all_links.append(
            (
                game_id,
                "reviews",
                f"https://tfgames.site/modules/viewgame/viewreviews.php?id={game_id}",
            )
        )

    result = defaultdict(dict)
    with ThreadPoolExecutor(max_workers=100) as executor:
        tasks = (
            executor.submit(fetch_page_raw, game_id, _type, url)
            for game_id, _type, url in all_links
        )

        with tqdm(total=len(all_links), unit="links") as pbar:
            for future in as_completed(tasks):
                pbar.update(1)
                game_id, _type, html = future.result()
                result[game_id][_type] = html

    console.log("Parsing info")
    games = []
    for game_id, data in tqdm(result.items(), unit="games"):
        game = parse_game_page(game_id, data["game"], data["reviews"])
        if game:
            games.append(game)

    console.log("Writing to database")
    for game in tqdm(games, unit="games"):
        pgame_to_db_game(cur, game)

    console.log("Done")


def main():
    console.log("Purging Database")
    db.drop_all_tables(with_all_data=True)
    db.create_tables()

    console.log("Starting Crawl")
    crawl()

    # TODO: Import sqlite DB into Postgres:
    # pgloader db.sqlite postgres://postgres:password@student-transfer.com:5432/tfgs


if __name__ == "__main__":
    main()
