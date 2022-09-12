# Project Intro

Sparkify is a new music streaming app. Sparkify's data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. The goal of this project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

## Python Scripts

There are two python scripts that are needed to run for this project. To run them, open a terminal and use the following commands in this order:

1. `python create_tables.py` - drops any old tables and creates all tables needed
2. `python etl.py` - loads the raw data from S3 into two staging tables and then processes the data and inserts into a star schema data warehouse

## Other files in the repo

1. `sql_queries.py` - all sql queries used to create and drop tables, and to insert the data into the tables
2. `dwh.cfg` - a config file with relevant information about data location, db definition etc.

## Database schema and ETL pipeline

This is a star schema database. It has one fact table, `factSongplay`, which represents music streaming events in sequence. It has several dimension tables enriching the data:

1. `dimSong` - information on each song
2. `dimArtist` - information on each artist
3. `dimUser` -  information on each user
4. `dimTime` - the event timestamp parsed into different time frames

The fact table and `dimTime` are sorted and distributed based on the timestamp field. `dimUser` is copied across all nodes due its small size (`diststyle all`), while `dimSong` and `dimArtist` are evenly distributed.

The ETL pipeline first processes the S3 json files into two staging tables, `events` and `songs`. As the next step, the `songs` table is used to create the dimSong and dimArtist tables, while events is used to create the fact table as well as `dimUser` and `dimTime`.

As part of the ETL creation of the fact table, the query tries to match artist information by looking at the dimension tables for matches on song title, artist name and song duration in order to have song_id and artist_id as foreign keys in the fact table. 

There are some duplicates in the data that might impact query results:

1. some artist_id in dimArtist are duplicates, that seem to happen when an artist appears under several names or with another artist
2. some user_id in dimUser are duplicates due to the user changing level from `free` to `paid` 


## Example queries

1. Top 20 songs by times played (duplicates are due to duplicate artist_id issue)

```
select
    title as song_title,
    name as artist_name,
    count(*) as times_played
from factSongplay f 
inner join dimSong s on f.song_id = s.song_id
inner join dimArtist a on f.artist_id = a.artist_id
group by title,name order by 3 desc limit 20
```

2. Top 5 artist by times played

```
select
    name as artist_name,
    count(*) as times_played
from factSongplay f 
inner join dimArtist a on f.artist_id = a.artist_id
group by name order by 2 desc limit 5
```

3. All songs played by a specific user in a specific month

```
select 
    t.start_time,
    s.title as song_title,
    a.name as artist_name,
    u.first_name as user_first_name,
    u.last_name as user_last_name,
    u.level as user_level
from factSongplay f
inner join dimTime t on f.start_time = t.start_time
inner join dimSong s on f.song_id = s.song_id
inner join dimArtist a on f.artist_id = a.artist_id
inner join dimUser u on f.user_id = u.user_id
where t.year = 2018 
and t.month = 11
and u.user_id = 8
order by 1
```

4. Top users in November 2018 by playing time (represented as sum of all songs' duration)

```
select 
    u.user_id,
    u.first_name as user_first_name,
    u.last_name as user_last_name,
    u.gender as user_gender,
    sum(duration) as total_listening_time
from factSongplay f
inner join dimTime t on f.start_time = t.start_time
inner join dimSong s on f.song_id = s.song_id
inner join dimUser u on f.user_id = u.user_id
where t.year = 2018 and t.month = 11
group by u.user_id,u.first_name,u.last_name,u.gender order by 5 desc
```