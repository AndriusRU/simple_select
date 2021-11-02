import sqlalchemy

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


db_connect = connection_database()
create_base(db_connect)

