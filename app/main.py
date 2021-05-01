### System ###
import os
import re
import math
import random
import shutil
import asyncio
import textwrap
import datetime as dt
from collections import defaultdict
from urllib.parse import urljoin, urlparse

### FastAPI ###
from typing import *
from fastapi_utils.tasks import repeat_every
from fastapi_contrib.pagination import Pagination
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, BaseSettings, Field
from fastapi import FastAPI, Depends, Request, BackgroundTasks, HTTPException

### Connectivity ###
import aiohttp

### Parsing ###
import arrow
from bs4 import BeautifulSoup

### Searching ###
from whoosh.index import create_in, open_dir
from whoosh.qparser.dateparse import DateParserPlugin
from whoosh.qparser import QueryParser, GtLtPlugin, FuzzyTermPlugin
from whoosh.fields import Schema, ID, TEXT, NUMERIC, DATETIME, BOOLEAN

### Database ###
from pony.orm import (
    Database,
    PrimaryKey,
    Required,
    db_session,
    select,
    count,
    desc,
    Optional as dOptional,
    Set as dSet,
)


class Settings(BaseSettings):
    DB_TYPE: str = "postgres"
    DB_FILE: str = None
    DATABASE_URL: str = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


config = Settings()


SEARCH_INDEX = open_dir("index")
SEARCHER = SEARCH_INDEX.searcher()
QUERY_PARSER = QueryParser("title", SEARCH_INDEX.schema)
QUERY_PARSER.add_plugin(GtLtPlugin())
QUERY_PARSER.add_plugin(FuzzyTermPlugin())
QUERY_PARSER.add_plugin(DateParserPlugin(free=True))


app = FastAPI(
    title="TFGS API",
    description=textwrap.dedent(
        """
    An unofficial REST API for TFGamesSite.
    This is currently a read-only API as you probably do not want to \
    share your credentals with an unknown third party.

    **This API is still a work in progress, so endpoints and output formats \
    are subject to change.**
    """
    ),
    version="1.0.0",
    openapi_tags=[
        {
            "name": "games",
            "description": "Operations on game entries in the database.",
        },
        {
            "name": "reviews",
            "description": "Operations on game reviews.",
        },
        {
            "name": "search",
            "description": "Search operations.",
        },
    ],
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


### Database ###


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
    note = dOptional(str)
    delete = dOptional(str)
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


### Models ###


class PReview(BaseModel):
    id: int
    author: str
    version: str
    date: dt.datetime
    text: str


class PReviewSearchResultPaginated(BaseModel):
    count: int
    next: dict
    previous: dict
    result: List[PReview]


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


class PGameResult(BaseModel):
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


class PGameResultPaginated(BaseModel):
    count: int
    next: dict
    previous: dict
    result: List[PGameResult]


class PGameSearchResult(BaseModel):
    id: int
    title: str
    likes: int


class PGameSearchResultPaginated(BaseModel):
    count: int
    next: dict
    previous: dict
    result: List[PGameSearchResult]


### Events ###


@repeat_every(seconds=60 * 60)  # 1 hour
def trigger_reindex():
    global SEARCHER
    print("Refreshing search engine index")
    SEARCHER = SEARCHER.refresh()


### Utility Methods ###


def db_game_to_pgame(game):
    themes = {}
    themes["adult"] = {theme.name: theme.id for theme in game.adult_themes}
    themes["transformation"] = {
        theme.name: theme.id for theme in game.transformation_themes
    }
    themes["multimedia"] = {theme.name: theme.id for theme in game.multimedia_themes}

    versions = defaultdict(list)
    for version in game.versions:
        for download in version.downloads:
            versions[version.version].append(download.to_dict(exclude="game_version"))

    reviews = []
    for review in game.reviews:
        reviews.append(db_review_to_preview(review))

    return PGame(
        id=game.id,
        title=game.title,
        authors={author.name: author.id for author in game.authors},
        version=game.version,
        game_engine=game.engine.name,
        content_rating=game.content_rating.name,
        language=game.language,
        release_date=game.release_date,
        last_update=game.last_update,
        development_stage=game.development_stage,
        likes=game.likes,
        contest=game.contest,
        orig_pc_gender=game.orig_pc_gender,
        themes=themes,
        thread=game.thread,
        versions=dict(versions),
        play_online=game.play_online,
        synopsis={"text": game.synopsis_text, "html": game.synopsis_html},
        plot={"text": game.plot_text, "html": game.plot_html},
        characters={"text": game.characters_text, "html": game.characters_html},
        walkthrough={"text": game.walkthrough_text, "html": game.walkthrough_html},
        changelog={"text": game.changelog_text, "html": game.changelog_html},
        reviews=reviews,
    )


def db_review_to_preview(review):
    return PReview(
        id=review.id,
        author=review.author,
        version=review.version,
        date=review.date,
        text=review.text,
    )


### Public Routes ###


class CustomPagination(Pagination):
    default_offset = 0
    default_limit = 10
    max_offset = 1000
    max_limit = 100


def paginate(request, total, data, offset, limit, **kwargs):
    if offset + limit >= total:
        next_url = None
    else:
        next_url = str(
            request.url.include_query_params(limit=limit, offset=offset + limit)
        )

    if offset <= 0:
        prev_url = None
    elif offset - limit <= 0:
        prev_url = str(request.url.remove_query_params(keys=["offset"]))
    else:
        prev_url = str(
            request.url.include_query_params(limit=limit, offset=offset - limit)
        )

    return {
        "count": len(data),
        "next": {
            "offset": offset + limit,
            "limit": limit,
            "url": next_url,
        },
        "previous": {
            "offset": max(offset - limit, 0),
            "limit": limit,
            "url": prev_url,
        },
        "result": data,
        **kwargs,
    }


@app.get(
    "/games/list",
    response_model=PGameResultPaginated,
    tags=["games"],
)
@db_session
def list_games(request: Request, pagination: CustomPagination = Depends()):
    game_count = count(g for g in Game)
    games = Game.select()[pagination.offset : pagination.offset + pagination.limit]
    data = [db_game_to_pgame(game) for game in games]
    return paginate(request, game_count, data, pagination.offset, pagination.limit)


@app.get("/games/list/new", response_model=List[PGameResult], tags=["games"])
@db_session
def recently_released():
    query = select(c for c in Game).order_by(lambda c: desc(c.release_date))[:10]
    data = [db_game_to_pgame(g) for g in query]
    return data


@app.get("/games/list/updated", response_model=List[PGameResult], tags=["games"])
@db_session
def recently_updated():
    query = select(c for c in Game).order_by(lambda c: desc(c.last_update))[:10]
    return [db_game_to_pgame(g) for g in query]


@app.get("/games/list/trending", response_model=List[PGameResult], tags=["games"])
@db_session
def trending_games():
    return [
        db_game_to_pgame(g)
        for g in select(g for g in Game if random.random() > 0.99)[:10]
    ]


@app.get("/games/{game_id}", response_model=PGame, tags=["games"])
@db_session
def show_game(game_id: int):
    game = Game.get(id=game_id)

    if not game:
        raise HTTPException(404, f"Game with ID {game_id} not found")

    return db_game_to_pgame(game)


@app.get(
    "/reviews/list/{game_id}",
    response_model=PReviewSearchResultPaginated,
    tags=["reviews"],
)
@db_session
def list_reviews(
    game_id: int, request: Request, pagination: CustomPagination = Depends()
):
    game = Game.get(id=game_id)

    if not game:
        raise HTTPException(404, f"Game with ID {game_id} not found")

    review_count = count(r for r in game.reviews)
    reviews = game.reviews.select()[
        pagination.offset : pagination.offset + pagination.limit
    ]
    data = [db_review_to_preview(r) for r in reviews]
    return paginate(request, review_count, data, pagination.offset, pagination.limit)


@app.get("/reviews/{review_id}", response_model=PReview, tags=["reviews"])
@db_session
def show_review(review_id: int):
    review = Review.get(id=review_id)

    if not review:
        raise HTTPException(404, f"Review with ID {review_id} not found")

    return db_review_to_preview(review)


@app.get("/search", response_model=PGameSearchResultPaginated, tags=["search"])
def search(query: str, request: Request, pagination: CustomPagination = Depends()):
    global SEARCHER, QUERY_PARSER
    found_items = []
    parsed_query = QUERY_PARSER.parse(query)

    suggestion = None
    corrected = SEARCHER.correct_query(parsed_query, query)
    if corrected.query != parsed_query:
        suggestion = corrected.string

    page_num = math.floor(pagination.offset / pagination.limit) + 1

    results = SEARCHER.search_page(
        parsed_query,
        pagenum=page_num,
        pagelen=pagination.limit,
    )

    if page_num > results.pagecount:
        raise HTTPException(404, "Requested page out of range")

    for hit in results:
        found_items.append(
            {"id": hit["id"], "title": hit["title"], "likes": hit["likes"]}
        )

    return paginate(
        request,
        results.total,
        found_items,
        pagination.offset,
        pagination.limit,
        suggestion=suggestion,
        page_num=results.pagenum,
        total_pages=results.pagecount,
    )
