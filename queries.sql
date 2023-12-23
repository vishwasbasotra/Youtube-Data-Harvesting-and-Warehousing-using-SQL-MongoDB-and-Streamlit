create database YoutubeDB;
use youtubedb;
create table ytChannel(
channel_id varchar(255) not null primary key,
channel_name varchar(255),
channel_type varchar(255),
channel_views int,
channel_description text,
channel_status varchar(255) 
);

create table Playlist(
playlist_id varchar(255)not null primary key,
channel_id varchar(255),
playlist_name varchar(255) 
);

create table video_comment(
comment_id varchar(255) not null primary key,
video_id varchar(255),
comment_text text,
comment_author varchar(255),
comment_published_date datetime 
);

create table video(
video_id varchar(255) not null primary key,
playlist_id varchar(255),
video_name varchar(255),
video_description text,
published_date datetime,
view_count int,
like_count int,
dislike_count int,
favourite_count int, 
comment_count int,
duration int,
thumbnail varchar(255),
caption_status varchar(255)
);

