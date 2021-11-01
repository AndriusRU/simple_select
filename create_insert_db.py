import json
from pprint import pprint
import requests
import sqlalchemy
from sqlalchemy import exc


# Connection to database
def connection_database():
    db = {
        'drivers': 'postgresql',
        'host': 'localhost',
        'port': '5432',
        'username': 'py44',
        'password': 'py44',
        'database': 'py44'
    }
    connection_string = f'{db.get("drivers")}://{db.get("username")}:{db.get("password")}@{db.get("host")}:{db.get("port")}/{db.get("database")}'
    engine = sqlalchemy.create_engine(connection_string)
    connection = engine.connect()
    print("Connection to PostgreSQL is successful")
    return connection


# Create Base
def create_base(connection):
    # Delete Schema
    connection.execute("""DROP SCHEMA IF EXISTS songs2 CASCADE;""")
    # Create Schema
    connection.execute("""CREATE SCHEMA IF NOT EXISTS songs2;""")

    # Create Table Genre
    connection.execute("""CREATE TABLE IF NOT EXISTS songs2.genre (
        id serial4 NOT NULL,
        genre_name varchar(100) NOT NULL,
        CONSTRAINT genre_pk PRIMARY KEY (id),
        CONSTRAINT genre_un UNIQUE (genre_name));""")

    # Create Table Country
    connection.execute("""CREATE TABLE IF NOT EXISTS songs2.country (
        id serial4 NOT NULL,
        country varchar(100) NOT NULL,
        CONSTRAINT country_pk PRIMARY KEY (id),
        CONSTRAINT country_fk UNIQUE (country));""")

    # Create table Collection
    connection.execute("""CREATE TABLE IF NOT EXISTS songs2.collections (
        id serial4 NOT NULL,
        collection_name varchar(100) NOT NULL,
        year_release int4 NOT NULL,
        CONSTRAINT collection_pk PRIMARY KEY (id));""")

    # Create table Albums
    connection.execute("""CREATE TABLE IF NOT EXISTS songs2.albums (
        id serial4 NOT NULL,
        album_name varchar(100) NOT NULL DEFAULT 'No name'::character varying,
        year_release int4 NOT NULL,
        CONSTRAINT albums_pk PRIMARY KEY (id));""")

    # Create table Singer
    connection.execute("""CREATE TABLE IF NOT EXISTS songs2.singer (
        id serial4 NOT NULL,
        singer_name varchar(100) NOT NULL,
        nickname varchar(100) NULL,
        country int4 NOT NULL,
        CONSTRAINT singer_pk PRIMARY KEY (id),
        CONSTRAINT singer_fk FOREIGN KEY (country) REFERENCES songs2.country(id));""")

    # Create table Tracks
    connection.execute("""CREATE TABLE IF NOT EXISTS songs2.tracks (
        id serial4 NOT NULL,
        track_name varchar(100) NOT NULL,
        duration int4 NOT NULL,
        album_id int4 NOT NULL,
        CONSTRAINT tracks_pk PRIMARY KEY (id),
        CONSTRAINT tracks_fk FOREIGN KEY (album_id) REFERENCES songs2.albums(id));""")

    # Create table rsSingerGenre
    connection.execute("""CREATE TABLE IF NOT EXISTS songs2.rsSingerGenre (
        id serial4 NOT NULL,
        singer_id int4 NOT NULL,
        genre_id int4 NOT NULL,	
        CONSTRAINT rsSingerGenre_pk PRIMARY KEY (id),
        CONSTRAINT rsSingerGenre_singer_fk FOREIGN KEY (singer_id) REFERENCES songs2.singer (id),
        CONSTRAINT rsSingerGenre_genre_fk FOREIGN KEY (genre_id) REFERENCES songs2.genre (id));""")

    # Create table rsSingerAlbum
    connection.execute("""CREATE TABLE IF NOT EXISTS songs2.rsSingerAlbum (
        id serial4 NOT NULL,
        singer_id int4 NOT NULL,
        album_id int4 NOT NULL,
        CONSTRAINT rsSingerAlbum_pk PRIMARY KEY (id),
        CONSTRAINT rsSingerAlbum_singer_fk FOREIGN KEY (singer_id) REFERENCES songs2.singer (id),
        CONSTRAINT rsSingerAlbum_album_fk FOREIGN KEY (album_id) REFERENCES songs2.albums (id));""")

    # Create table rsCollectionTrack
    connection.execute("""CREATE TABLE IF NOT EXISTS songs2.rsCollectionTrack (
        id serial4 NOT NULL,
        collection_id int4 NOT NULL,
        track_id int4 NOT NULL,
        CONSTRAINT rsCollectionTrack_pk PRIMARY KEY (id),
        CONSTRAINT rsCollectionTrack_collection_fk FOREIGN KEY (collection_id) REFERENCES songs2.collections (id),
        CONSTRAINT rsCollectionTrack_track_fk FOREIGN KEY (track_id) REFERENCES songs2.tracks (id));""")


# return ID from table, if this value exist. Otherwise, None
def is_exist(db, table, dict_field_value):
    str_condition = ''
    i = 1
    for field, value in dict_field_value.items():
        if isinstance(value, str):
            str_condition += f"{field} = '{value}'"
        else:
            str_condition += f"{field} = {value}"
        if i < len(dict_field_value):
            str_condition += " AND "
        i += 1
    # print(str_condition)

    try:
        return db.execute(f"SELECT id from songs2.{table} WHERE {str_condition};").fetchall()[0][0]
    except:
        return None


def insert_albums(db, dict_album):
    if is_exist(db, 'albums', dict_album) is None:
        db.execute(f"""INSERT INTO songs2.albums (album_name, year_release) 
                        VALUES ('{dict_album.get('album_name')}', {dict_album.get('year_release')});""")


def insert_collections(db, dict_col):
    if is_exist(db, 'collections', dict_col) is None:
        db.execute(f"""INSERT INTO songs2.collections (collection_name, year_release) 
                        VALUES ('{dict_col.get('collection_name')}', {dict_col.get('year_release')});""")


def insert_singer(db, dict_singer):
    if is_exist(db, 'singer', dict_singer) is None:
        db.execute(f"""INSERT INTO songs2.singer (singer_name, nickname, country) 
                        VALUES ('{dict_singer.get('singer_name')}', '{dict_singer.get('nickname')}', 
                                 {dict_singer.get('country')});""")


def insert_rsgenresinger(db, dict_gsinger):
    if is_exist(db, 'rsSingerGenre', dict_gsinger) is None:
        db.execute(f"""INSERT INTO songs2.rsSingerGenre (singer_id, genre_id) 
                        VALUES ({dict_gsinger.get('singer_id')}, {dict_gsinger.get('genre_id')});""")


def insert_rssingeralbum(db, dict_alsinger):
    if is_exist(db, 'rsSingerAlbum', dict_alsinger) is None:
        db.execute(f"""INSERT INTO songs2.rsSingerAlbum (singer_id, album_id) 
                        VALUES ({dict_alsinger.get('singer_id')}, {dict_alsinger.get('album_id')});""")


def insert_rscollectiontrack(db, dict_coltrack):
    if is_exist(db, 'rsSCollectionTrack', dict_coltrack) is None:
        db.execute(f"""INSERT INTO songs2.rsCollectionTrack (collection_id, track_id) 
                        VALUES ({dict_coltrack.get('collection_id')}, {dict_coltrack.get('track_id')});""")


def insert_track(db, dict_track):
    if is_exist(db, 'tracks', dict_track) is None:
        db.execute(f"""INSERT INTO songs2.tracks (track_name, duration, album_id) 
                        VALUES ('{dict_track.get('track_name')}', {dict_track.get('duration')},
                         {dict_track.get('album_id')});""")


def insert_data(db, list_dict):
    for elem in list_dict:
        track_name = elem.get("track").get("name")
        duration = elem.get("track").get("duration")
        for singer_item in elem.get("artists"):
            country = is_exist(db, 'country', {"country": singer_item.get("country")})
            singer = {"singer_name": singer_item.get("name"), "nickname": singer_item.get("nickname"), "country": country}
            insert_singer(db, singer)
            singer_id = is_exist(db, 'singer', singer)

            for genre_elem in singer_item.get("genres"):
                genre_id = is_exist(db, 'genre', {"genre_name": genre_elem})
                insert_rsgenresinger(db, {"singer_id": singer_id, "genre_id": genre_id})

            for item_elem in elem.get("albums"):
                if item_elem.get('type') == 'album':
                    album = {"album_name": item_elem.get("name"), "year_release": item_elem.get("year")}
                    insert_albums(db, album)
                    album_id = is_exist(db, 'albums', album)
                    insert_rssingeralbum(db, {"singer_id": singer_id, "album_id": album_id})
                    insert_track(db, {"track_name": track_name, "duration": duration, "album_id": album_id})
                elif item_elem.get('type') == 'collection':
                    album = {"collection_name": item_elem.get("name"), "year_release": item_elem.get("year")}
                    insert_collections(db, album)
                    collection_id = is_exist(db, 'collections', album)
                    track_id = is_exist(db, 'tracks', {"track_name": track_name, "duration": duration})
                    insert_rscollectiontrack(db, {"collection_id": collection_id, "track_id": track_id})


db_connect = connection_database()
create_base(db_connect)

# Insert data to Genre from txt file
with open('genres.txt', 'r', encoding='utf-8') as file_genres:
    for line in file_genres:
        db_connect.execute(f"INSERT INTO songs2.genre(genre_name) VALUES ('{line.strip()}');")

# # Read Country from URL
url = 'https://namaztimes.kz/ru/api/country?type=json'
response = requests.get(url).json()
set_country = set()
for item in response.values():
    set_country.add(item)
# Insert data to Country from response
for item in set_country:
    db_connect.execute(f"INSERT INTO songs2.country(country) VALUES ('{item.strip()}');")

with open('songs.json', encoding='utf-8') as file_songs:
    data_songs = json.load(file_songs)

insert_data(db_connect, data_songs.get("songs"))

