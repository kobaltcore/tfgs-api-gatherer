### System ##
import os
import shutil
import datetime as dt
from urllib.parse import urlparse

### Date Handling ###
import arrow

### Pydantic ###
from pydantic import BaseSettings

### Searching ##
from whoosh.index import create_in
from whoosh.fields import Schema, ID, TEXT, NUMERIC, DATETIME, BOOLEAN

### Database ###
from pony.orm import (
    Database,
    PrimaryKey,
    Required,
    db_session,
    Optional as dOptional,
    Set as dSet,
)


class Settings(BaseSettings):
    DB_TYPE: str = "postgres"
    DB_FILE: str = None
    DATABASE_URL: str = None
    BASE_URL: str = "https://tfgames.site"
    SYSTEM_KEY: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


config = Settings()

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


schema = Schema(
    id=ID(stored=True, sortable=True),
    title=TEXT(stored=True),
    synopsis=TEXT,
    likes=NUMERIC(stored=True, sortable=True),
    last_update=DATETIME(sortable=True),
    release_date=DATETIME(sortable=True),
    play_online=BOOLEAN,
)


def main():
    if os.path.exists("index"):
        print("Removing existing Index")
        shutil.rmtree("index")

    print("Creating Index")
    os.mkdir("index")
    IX = create_in("index", schema)

    print("Filling Index")
    writer = IX.writer()
    with db_session:
        for game in Game.select():
            writer.add_document(
                id=str(game.id),
                title=game.title,
                synopsis=game.synopsis_text,
                likes=game.likes,
                last_update=arrow.get(game.last_update).datetime,
                release_date=arrow.get(game.release_date).datetime,
                play_online=game.play_online != "",
            )
    writer.commit()

    print("Done")


if __name__ == "__main__":
    main()
