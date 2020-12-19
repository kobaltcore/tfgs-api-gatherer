### System ###
import os
import textwrap
import datetime as dt
from urllib.parse import urlparse
from collections import defaultdict

### FastAPI ###
from typing import *
from fastapi_contrib.pagination import Pagination
from pydantic import BaseModel, BaseSettings, Field
from fastapi import FastAPI, Depends, BackgroundTasks  # , HTTPException

### Security ###
# from jose import JWTError, jwt
# from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm


### Database ###
from pony.orm import (
    Database,
    PrimaryKey,
    Required,
    db_session,
    select,
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
    ],
    redoc_url=None,
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
else:
    parsed = urlparse(config.DATABASE_URL)
    db.bind(
        provider="postgres",
        user=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port,
        database=parsed.path.lstrip("/"),
    )

db.generate_mapping(create_tables=True)


### Models ###


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


class Topic(BaseModel):
    title: str
    last_author: str
    last_post_time: dt.datetime


class User(BaseModel):
    username: str
    groups: List[str]
    joined: dt.datetime
    warnings: int
    total_posts: int


class TFGSAuthInfo(BaseModel):
    __cfduid: str
    phpbb3_tfgs_u: str
    phpbb3_tfgs_sid: str
    phpbb3_tfgs_k: Optional[str]


class UserWithTFGSAuthInfo(User, TFGSAuthInfo):
    pass


class Token(BaseModel):
    access_token: str
    token_type: str


### Events ###


# @app.on_event("startup")
# def startup_event():
#     print(f"Config: {config}")


# @app.on_event("shutdown")
# def shutdown_event():
#     pass


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
        reviews.append(
            PReview(
                id=review.id,
                author=review.author,
                version=review.version,
                date=review.date,
                text=review.text,
            )
        )

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


@app.get(
    "/games/list",
    response_model=List[PGameSearchResult],
    tags=["games"],
)
def show_games(pagination: Pagination = Depends()):
    """
    List all games in the TFGS database.
    """
    with db_session:
        games = Game.select()[pagination.offset : pagination.limit]
        return [db_game_to_pgame(game) for game in games]


@app.get("/games/{game_id}", response_model=PGameReduced, tags=["games"])
def show_game(game_id: int):
    """
    Show more detailed data for a specific game.
    """
    with db_session:
        game = Game.get(id=game_id)
        return db_game_to_pgame(game)


@app.get(
    "/reviews/{game_id}/list",
    response_model=List[PReview],
    tags=["reviews"],
)
def list_reviews(game_id: int, pagination: Pagination = Depends()):
    """
    List all reviews for a specific game.
    """
    with db_session:
        reviews = Game.get(id=game_id).reviews.select()[
            pagination.offset : pagination.limit
        ]
        return [db_review_to_preview(r) for r in reviews]


@app.get("/reviews/{review_id}", response_model=PReview, tags=["reviews"])
def show_review(review_id: int):
    """
    Show a specific review for a specific game.
    """
    with db_session:
        review = Review.get(id=review_id)
        return db_review_to_preview(review)


@app.get("/search", response_model=List[PGameSearchResult], tags=["search"])
def search(
    text: str = "",
    likes_min: int = 0,
    likes_max: int = 10_000,
    play_online: bool = None,
):
    """
    Show a specific review for a specific game.
    """
    term = text.lower()
    with db_session:
        query = select(
            c
            for c in Game
            if (
                term in c.title.lower()
                or term in c.synopsis_text.lower()
                or term in c.plot_text.lower()
                or term in c.characters_text.lower()
                or term in c.walkthrough_text.lower()
                or term in c.changelog_text.lower()
            )
            and c.likes <= likes_max
            and c.likes >= likes_min
        )
        if play_online is not None:
            if play_online:
                query = select(c for c in query if c.play_online != "")
            else:
                query = select(c for c in query if c.play_online == "")
        games = [db_game_to_pgame(g) for g in query]
        return games


@app.get("/recent", response_model=List[PGameSearchResult], tags=["search"])
def recent_updates(past_weeks: float = 0, past_days: float = 0, past_hours: float = 0):
    """
    Show a specific review for a specific game.
    """
    delta = dt.timedelta(days=past_weeks * 7 + past_days, hours=past_hours)
    with db_session:
        query = select(
            c for c in Game if c.last_update >= (dt.datetime.utcnow() - delta)
        ).order_by(lambda c: c.last_update)
        games = [db_game_to_pgame(g) for g in query]
        return games
