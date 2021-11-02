from pprint import pprint
import sqlalchemy

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


db_connect = connection_database()

# название и год выхода альбомов, вышедших в 2018 году
query_result = db_connect.execute(f"""SELECT album_name, year_release FROM songs2.albums
                                        WHERE year_release = 2018;""").fetchall()
pprint(query_result)

# название и продолжительность самого длительного трека;
query_result = db_connect.execute(f"""SELECT track_name, duration FROM songs2.tracks
                                        ORDER BY duration DESC LIMIT 1;""").fetchall()
pprint(query_result)

# второй вариант
query_result = db_connect.execute(f"""SELECT track_name, duration FROM songs2.tracks
                                        WHERE duration = (SELECT max(duration) FROM songs2.tracks);""").fetchall()
pprint(query_result)

# название треков, продолжительность которых не менее 3,5 минуты;
query_result = db_connect.execute(f"""SELECT track_name FROM songs2.tracks
                                        WHERE duration >= 3*60+30;""").fetchall()
pprint(query_result)

# названия сборников, вышедших в период с 2018 по 2020 год включительно;
query_result = db_connect.execute(f"""SELECT collection_name FROM songs2.collections
                                        WHERE year_release between 2018 and 2020;""").fetchall()
pprint(query_result)

# исполнители, чье имя состоит из 1 слова;
query_result = db_connect.execute(f"""SELECT singer_name FROM songs2.singer
                                        WHERE singer_name NOT LIKE '%% %%';""").fetchall()
pprint(query_result)

# название треков, которые содержат слово "мой"/"my"
query_result = db_connect.execute(f"""SELECT track_name FROM songs2.tracks
                                        WHERE track_name like '%% my %%' OR track_name like '%% мой %%';""").fetchall()
pprint(query_result)
