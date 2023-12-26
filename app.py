import streamlit as st
from googleapiclient.discovery import build
import pandas as pd
from pymongo import MongoClient
from pymongo.mongo_client import MongoClient
import sqlalchemy as sa
import mysql.connector
from mysql.connector import Error
from streamlit_option_menu import option_menu
import plotly.express as px

#Configuring streamlit header
st.set_page_config(page_title="Youtube Data Harvesting and Warehousing", page_icon=':bar_chart:', layout='wide')

# header section
st.title(":red[Youtube Dataharvesting and Warehousing:]")

#class to extract data from youtube
class youtubeExtract:
    #getting basic info about the channel
    def get_channel_stats(youtube, channel_id):
        request = youtube.channels().list(part='snippet, contentDetails, statistics, contentDetails', id=channel_id)
        response = request.execute()
        data = dict(
                        channel_name= response['items'][0]['snippet']['title'],
                        channel_id=response['items'][0]['id'],
                        subscriber_count= response['items'][0]['statistics']['subscriberCount'],
                        channel_views= response['items'][0]['statistics']['viewCount'],
                        total_videos= response['items'][0]['statistics']['videoCount'],
                        channel_description= response['items'][0]['snippet']['description'],
                        upload_id= response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                )
        return data
    
    #getting basic info about the channel playlists such as playlist_name, playlist_id, channel_id
    def get_channel_playlists(youtube, channel_id):

        request = youtube.playlists().list(part='snippet, contentDetails', maxResults=10, channelId=channel_id)
        response = request.execute()
        playlists_ids = {}
        for i in range(0, len(response['items'])):
            data = dict(
                            playlist_name= response['items'][i]['snippet']['title'],
                            playlist_id=response['items'][i]['id'],
                            channel_id=response['items'][i]['snippet']['channelId'])
            playlists_ids["Playlist_no_"+str(i+1)] = data
        return playlists_ids
    
    #getting total the video ids from the channel
    def get_video_ids(youtube, upload_id):
        
        request = youtube.playlistItems().list(part='contentDetails', playlistId=upload_id, maxResults=50)
        response = request.execute()
        video_ids = []
        for i in range(0, len(response['items'])):
            data = response['items'][i]['contentDetails']['videoId']
            video_ids.append(data)
        return video_ids
    
    #getting basic stats about the channel videos such as video_name, video_id, video_description, channel_id etc
                    
    def get_video_stats(youtube, video_ids):
        
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_ids
        )
        response = request.execute()
        video_stats = {}
        #converting the PM2H11M24S to 2:11:24 format using pandas Timedelta function
        def time_duration(t):
                a = pd.Timedelta(t)
                b = str(a).split()[-1]
                return b
        
        for i in range(0, len(response['items'])):
            data = dict(
                        video_name= response['items'][i]['snippet']['title'],
                    video_id=response['items'][i]['id'],
                    video_description= response['items'][i]['snippet']['description'],
                    channel_id=response['items'][i]['snippet']['channelId'],
                    published_date=response['items'][i]['snippet']['publishedAt'][0:10],
                    published_time=response['items'][i]['snippet']['publishedAt'][11:19],
                    view_count=response['items'][i]['statistics']['viewCount'],
                    like_count=response['items'][i]['statistics']['likeCount'],
                    favourite_count=response['items'][i]['statistics']['favoriteCount'],
                    comment_count=response['items'][i]['statistics']['commentCount'],
                    duration=time_duration(response['items'][i]['contentDetails']['duration']),
                    thumbnail=response['items'][i]['snippet']['thumbnails']['default']['url'],
                    caption_status=response['items'][i]['contentDetails']['caption'])
            video_stats["video_no_"+str(i+1)] = data
        return video_stats
    
    #getting stats about the video comments such as comment_id, comment_text, comment_author etc
    def comments(youtube, video_id):
        #getting stats about the video comments
        request = youtube.commentThreads().list(
            part='id, snippet',
            videoId=video_id,
            maxResults=100
        )
        response = request.execute()
        comments = {}
        for i in range(0, len(response['items'])):
            data  = dict(
                    comment_id=response['items'][i]['id'],
                    comment_text= response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author=response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    published_date=response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][0:10],
                    published_time=response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][11:19],
                    video_id=response['items'][i]['snippet']['videoId']
                    )
            comments["comment_no_"+str(i+1)] = data
        return comments
    
    #this fucntion is used to display some sample data on the display
    def display_sample_data(channel_id):
        data = youtubeExtract.main(channel_id)
        youtubeData = {
                        "channel_name": data["channel_name"],
                        "playlists": data["playlists"],
                        "videos": data["videos"]["video_no_1"],
                        "comments": data["comments"]["video_0"]
                        }
        return youtubeData
    
    def main(channel_id):
        channel_stats = youtubeExtract.get_channel_stats(youtube, enteredChannel_id)
        upload_id = channel_stats['upload_id']
        playlists_stats = youtubeExtract.get_channel_playlists(youtube, channel_id)
        video_ids = youtubeExtract.get_video_ids(youtube, upload_id)
        video_stats = youtubeExtract.get_video_stats(youtube, video_ids)
        comment_stats = {}
        j=0
        for i in video_ids:
            comment_stats["video_"+str(j)] = youtubeExtract.comments(youtube, i)
            j=j+1
        youtubeData = {
                        "channel_name": channel_stats,
                        "playlists": playlists_stats,
                        "videos": video_stats,
                        "comments": comment_stats
                        }
        return youtubeData

# class to export data to MongoDB    
class mongodb:

    #uploading a channel json as collection and return status as if it's uploaded or not
    def toMongodb():
        vishwas = MongoClient("mongodb+srv://basotra:yAAipVdBtK8gbRxl@cluster0.nvekqq7.mongodb.net/?retryWrites=true&w=majority")
        db = vishwas['YT_database']
        main_collection = db.ytExtracted_collection
        temp_collection = db.temp
        channel_ids = []
        status,channel_id = '',''

        #if mognodb is empty the channel json will be directly upload to mongodb
        if list(main_collection.find()) == []:
            for i in temp_collection.find():
                main_collection.insert_many([i])
            st.success('Database Collection uploaded')
            st.balloons()
            mongodb.droptemp_collection()
        #if mognodb is not empty it will check for some condition like if it's already their or not
        else:

            #getting all the channel_id's in the mongodb 
            for i in main_collection.find():
                temp = i['channel_name']['channel_id']
                channel_ids.append(temp)
            
            for i in temp_collection.find():
                #check if the newly extracted data is already in the mongodb
                if i['channel_name']['channel_id'] in channel_ids:
                    status = "Data collection already exist"
                    channel_id = i['channel_name']['channel_id']

                #check if the newly extracted data is not in the mongodb, then it will be uploaded
                else:
                    for i in temp_collection.find():
                        main_collection.insert_many([i])
                    status = "Database Collection uploaded"
                    st.success('Database Collection uploaded')
                    st.balloons()
                    mongodb.droptemp_collection()
        return status,channel_id
    
    # this function will upload the youtube data as a temp collection
    def tempMongodb(youtubeData_json):
        vishwas = MongoClient("mongodb+srv://basotra:yAAipVdBtK8gbRxl@cluster0.nvekqq7.mongodb.net/?retryWrites=true&w=majority")
        db = vishwas['YT_database']
        collection = db.temp
        collection.insert_many([youtubeData_json])
        status = "Database Collection uploaded"
        return status
    
    # this is to delete the temp collection that was created earlier
    def droptemp_collection():
        vishwas = MongoClient("mongodb+srv://basotra:yAAipVdBtK8gbRxl@cluster0.nvekqq7.mongodb.net/?retryWrites=true&w=majority")
        db = vishwas['YT_database']
        temp_collection = db.temp
        temp_collection.drop()

    # this to delete a specified collection from the mongodb with the matching _id
    def drop_collection(channel_id):
        vishwas = MongoClient("mongodb+srv://basotra:yAAipVdBtK8gbRxl@cluster0.nvekqq7.mongodb.net/?retryWrites=true&w=majority")
        db = vishwas['YT_database']
        collection = db.ytExtracted_collection
        collection.delete_one({'_id':channel_id})
    

    def main(youtubeData_json):
        status,channel_id = mongodb.toMongodb(youtubeData_json)
        return status, channel_id

#class to migrate data from mongoDB to MySQL
class mysqlData():

    #function to retrieve the specfic collection from the mongodb
    def reading_MongoDB(index):
        vishwas = MongoClient("mongodb+srv://basotra:yAAipVdBtK8gbRxl@cluster0.nvekqq7.mongodb.net/?retryWrites=true&w=majority")
        db = vishwas['YT_database']
        collection = db.ytExtracted_collection
        #retireved_mongodb_data = []
        index_count = 0
        for i in collection.find():
            retireved_mongodb_data = i
            if index_count == index:
                break
            index_count = index_count + 1
        return retireved_mongodb_data
    
    #function to create a channel df
    def channels_df(data):
        df = pd.DataFrame([data["channel_name"]])
        return df
    
    #function to create a playlists df
    def playlists_df(data):
        plist_df = dict.fromkeys(["playlist_name", "playlist_id","channel_id"], )
        df = pd.DataFrame([plist_df])

        #deleting everything from the dataframe
        df = df.iloc[1:, :]
        for i in range(0, len(data["playlists"])):
            current_playlist = list(data["playlists"].keys())[i]
            plist_df["playlist_name"] = data["playlists"][current_playlist]["playlist_name"]
            plist_df["playlist_id"] = data["playlists"][current_playlist]["playlist_id"]
            plist_df["channel_id"] = data["playlists"][current_playlist]["channel_id"]
            temp_df = pd.DataFrame([plist_df])
            df=pd.concat([df, temp_df], ignore_index=True)
        return df
    
    #function to create a videos df
    def videos_df(data):
        vid_df = dict.fromkeys(["video_name", 
                        "video_id",
                        "video_description", 
                        "channel_id", 
                        "published_date", 
                        "published_time", 
                        "view_count", 
                        "like_count", 
                        "favourite_count",
                        "comment_count",
                       "duration", 
                       "thumbnail",
                       "caption_status"], )
        df = pd.DataFrame([vid_df])

        #deleting everything from the dataframe
        df = df.iloc[1:, :]
        for i in range(0, len(data["videos"])):
            current_video = list(data["videos"].keys())[i]
            vid_df["video_name"] = data["videos"][current_video]["video_name"]
            vid_df["video_id"] = data["videos"][current_video]["video_id"]
            vid_df["video_description"] = data["videos"][current_video]["video_description"]
            vid_df["channel_id"] = data["videos"][current_video]["channel_id"]
            vid_df["published_date"] = data["videos"][current_video]["published_date"]
            vid_df["published_time"] = data["videos"][current_video]["published_time"]
            vid_df["view_count"] = data["videos"][current_video]["view_count"]
            vid_df["like_count"] = data["videos"][current_video]["like_count"]
            vid_df["favourite_count"] = data["videos"][current_video]["favourite_count"]
            vid_df["comment_count"] = data["videos"][current_video]["comment_count"]
            vid_df["duration"] = data["videos"][current_video]["duration"]
            vid_df["thumbnail"] = data["videos"][current_video]["thumbnail"]
            vid_df["caption_status"] = data["videos"][current_video]["caption_status"]
            temp_df = pd.DataFrame([vid_df])
            df=pd.concat([df, temp_df],ignore_index=True)
        return df
    
    #function to create a comments df
    def comments_df(data):
        total_videos = list(data["comments"].keys())
        c_df = dict.fromkeys(["comment_id", "video_id","comment_text", "comment_author", "published_date"], )
        df = pd.DataFrame([c_df])

        #deleting everything from the dataframe
        df = df.iloc[1:, :]
        for i in total_videos:
            for keys in data["comments"][i].keys():
                c_df["comment_id"] = data["comments"][i][keys]["comment_id"]
                c_df["video_id"] = data["comments"][i][keys]["video_id"]
                c_df["comment_text"] = data["comments"][i][keys]["comment_text"]
                c_df["comment_author"] = data["comments"][i][keys]["comment_author"]
                c_df["published_date"] = data["comments"][i][keys]["published_date"]
                temp_df = pd.DataFrame([c_df])
                df=pd.concat([df, temp_df], ignore_index=True)
        return df
    
    #function to migrate channel df to MySQL
    def channels_df_tosql(df):
        myslq_engine = sa.create_engine('mysql+pymysql://root:admin@localhost:3306/youtubedb')
        df.to_sql(name='channel_table',
                  con=myslq_engine,
                  dtype={'channel_name':sa.types.VARCHAR(length=255),
                         'channel_id':sa.types.VARCHAR(length=255),
                         'subscriber_count':sa.types.INTEGER(),
                         'channel_views':sa.types.INTEGER(),
                         'total_videos':sa.types.INTEGER(),
                         'channel_description':sa.types.TEXT(),
                         'upload_id':sa.types.VARCHAR(length=255)
                        },
                  if_exists='append'
                 )
    
    #function to migrate playlists df to MySQL
    def playlists_df_tosql(df):
        myslq_engine = sa.create_engine('mysql+pymysql://root:admin@localhost:3306/youtubedb')
        df.to_sql(name='playlist_table',
                  con=myslq_engine,
                  dtype={'playlist_name':sa.types.TEXT(),
                         'channel_id':sa.types.VARCHAR(length=255),
                         'playlist_id':sa.types.VARCHAR(length=255)
                        },
                    if_exists='append'
                 )
    
    #function to migrate videos df to MySQL
    def videos_df_tosql(df):
        myslq_engine = sa.create_engine('mysql+pymysql://root:admin@localhost:3306/youtubedb')
        df.to_sql(name='videos_table',
                 con=myslq_engine,
                 dtype={'video_name':sa.types.TEXT(),
                         'video_id':sa.types.VARCHAR(length=255),
                         'video_description':sa.types.TEXT(),
                         'channel_id':sa.types.VARCHAR(length=255),
                         'published_date':sa.types.DATE(),
                         'published_time':sa.types.TIME(),
                         'view_count':sa.types.INTEGER(),
                        'like_count':sa.types.INTEGER(),
                        'favourite_count':sa.types.INTEGER(),
                        'comment_count':sa.types.INTEGER(),
                        'duration':sa.types.TIME(),
                        'thumbnail':sa.types.VARCHAR(length=255),
                        'caption_status':sa.types.VARCHAR(length=255)
                        },
                 if_exists='append'
                 )
    
    #function to migrate comments df to MySQL
    def comments_df_tosql(df):
        myslq_engine = sa.create_engine('mysql+pymysql://root:admin@localhost:3306/youtubedb')
        df.to_sql(name='comments_table',
                  con=myslq_engine,
                  dtype={'comment_id':sa.types.VARCHAR(length=255),
                         'video_id':sa.types.VARCHAR(length=255),
                         'comment_text':sa.types.TEXT(),
                         'comment_author':sa.types.VARCHAR(length=255),
                         'published_date':sa.types.DATE(),
                        },
                   if_exists='append'
                 )
    
    #function to delete the duplicate rows from the table
    def deleteduplicateRows():
        myslq_engine = mysql.connector.connect(user='root', 
                                       password='admin', 
                                       host='localhost', 
                                       port='3306', 
                                       database = 'youtubedb')

        crsr = myslq_engine.cursor()
        
        # SQL command to create a table in the database
        sql_command = """SELECT videos_table.video_name as "Video Name" , 
                        channel_table.channel_name as "Channel Name" FROM videos_table
                        left join channel_table On videos_table.channel_id = channel_table.channel_id;"""
        
        # execute the statement
        crsr.execute(sql_command)

    def main(index):
        mongoDB_data = mysqlData.reading_MongoDB(index)

        #creating dataframes from mongodb collection data
        channels_df = mysqlData.channels_df(mongoDB_data)
        playlists_df = mysqlData.playlists_df(mongoDB_data)
        videos_df = mysqlData.videos_df(mongoDB_data)
        comments_df = mysqlData.comments_df(mongoDB_data)
        #mysqlData.deleteduplicateRows()

        #now migrating dataframes to mysql
        mysqlData.channels_df_tosql(channels_df)
        mysqlData.playlists_df_tosql(playlists_df)
        mysqlData.videos_df_tosql(videos_df)
        mysqlData.comments_df_tosql(comments_df)

#class to analyse the data retrieved from MySQL
class dataAnalysis:

    #function to get list of channels from the channel table
    def list_channel_names():
        try:
            myslq_engine = mysql.connector.connect(user='root', 
                                        password='admin', 
                                        host='localhost', 
                                        port='3306', 
                                        database = 'youtubedb')
            cursor = myslq_engine.cursor()
            cursor.execute("select channel_name from channel_table")
            channel_list = cursor.fetchall()
            channel_list = [i[0] for i in channel_list]
            channel_list.sort(reverse=False)
            return channel_list
        
        except Error as e:
            print("Error while connecting to MySQL", e)

    #function to get channel names as dataframe from channel table
    def totalChannels():
        try:
            myslq_engine = mysql.connector.connect(user='root', 
                                       password='admin', 
                                       host='localhost', 
                                       port='3306', 
                                       database = 'youtubedb')

            crsr = myslq_engine.cursor()
            
            # SQL query to be executed in the database
            sql_command = """select channel_name as "Channel Name" from channel_table;"""
            
            # execute the statement
            crsr.execute(sql_command)
            queryResult = crsr.fetchall()

            #adding result to the dataframe
            i = [i for i in range(1, len(queryResult)+1)]
            df = pd.DataFrame(queryResult, columns=['Channel Name'], index=i)
            df = df.rename_axis('S.No')
            myslq_engine.close()
            return df
        
        except Error as e:
            print("Error while connecting to MySQL", e)

    #function to get playlist names corresponding to their channel name
    def channelwise_Playlists():
        try:
            myslq_engine = mysql.connector.connect(user='root', 
                                        password='admin', 
                                        host='localhost', 
                                        port='3306', 
                                        database = 'youtubedb')

            crsr = myslq_engine.cursor()
            
            # SQL query to be executed in the database
            sql_command = """select channel_table.channel_name as "Channel Name", playlist_table.playlist_name as "Playlist Name"
            FROM playlist_table
            left join channel_table on playlist_table.channel_id = channel_table.channel_id;
            """
            
            # execute the statement
            crsr.execute(sql_command)
            queryResult = crsr.fetchall()

            #adding result to the dataframe
            i = [i for i in range(1, len(queryResult)+1)]
            df = pd.DataFrame(queryResult, columns=['Channel Name', 'Playlist Name'], index=i)
            df = df.rename_axis('S.No')
            myslq_engine.close()
            return df
        
        except Error as e:
            print("Error while connecting to MySQL", e)
    
    #function to get playlist names corresponding to the selected channel name
    def selectedChannel_Playlists(selectedChannel):
        try:
            myslq_engine = mysql.connector.connect(user='root', 
                                        password='admin', 
                                        host='localhost', 
                                        port='3306', 
                                        database = 'youtubedb')

            crsr = myslq_engine.cursor()
            
            # SQL query to be executed in the database
            sql_command = f"""select channel_table.channel_name as 'Channel Name', playlist_table.playlist_name as 'Playlist Name'FROM playlist_table
            left join channel_table on playlist_table.channel_id = channel_table.channel_id
            where channel_table.channel_name = '{selectedChannel}';
            """
            
            # execute the statement
            crsr.execute(sql_command)
            queryResult = crsr.fetchall()

            #adding result to the dataframe
            i = [i for i in range(1, len(queryResult)+1)]
            df = pd.DataFrame(queryResult, columns=['Channel Name', 'Playlist Name'], index=i)
            df = df.rename_axis('S.No')
            myslq_engine.close()
            return df
        
        except Error as e:
            print("Error while connecting to MySQL", e)

    #function to get total playlists corresponding to their channel name
    def totalPlaylists():
        try:
            myslq_engine = mysql.connector.connect(user='root', 
                                        password='admin', 
                                        host='localhost', 
                                        port='3306', 
                                        database = 'youtubedb')

            crsr = myslq_engine.cursor()
            
            # SQL query to be executed in the database
            sql_command = """select channel_table.channel_name as "Channel Name", count(playlist_table.playlist_id) as "Total Playlists"
                            FROM playlist_table
                            left join channel_table on playlist_table.channel_id = channel_table.channel_id
                            group by channel_table.channel_name
                            order by  count(playlist_table.playlist_id) desc;
                        """
            
            # execute the statement
            crsr.execute(sql_command)
            queryResult = crsr.fetchall()

            #adding result to the dataframe
            i = [i for i in range(1, len(queryResult)+1)]
            df = pd.DataFrame(queryResult, columns=['Channel Name', 'Total Playlists'], index=i)
            df = df.rename_axis('S.No')
            myslq_engine.close()
            return df
        
        except Error as e:
            print("Error while connecting to MySQL", e)

    def main():
        channel_list = dataAnalysis.list_channel_names()
        if channel_list == []:
            st.info("The SQL database is currently empty")
        else:

            col1, col2 = st.columns(2)
            with col1:
                totalChannels_df = dataAnalysis.totalChannels()
                st.subheader("Total Youtube Channels:")
                st.dataframe(totalChannels_df, width=500)
            with col2:
                st.subheader("Channel Wise Playlists:")
                channel_list.insert(0, "Overall")
                selected_option = st.selectbox(
                'Please select the channel data you want to migrate to MySQL:',
                (channel_list) ,placeholder='Select',label_visibility='hidden')

                if selected_option == 'Overall':
                    channelwise_playlists_df = dataAnalysis.channelwise_Playlists()
                    st.subheader("Channel Wise Playlists:")
                    st.dataframe(channelwise_playlists_df)
                else:
                    channelwise_playlists_df = dataAnalysis.selectedChannel_Playlists(selected_option)
                    st.subheader("Channel Wise Playlists:")
                    st.dataframe(channelwise_playlists_df, width=500)
            col3, col4 = st.columns([1,2])
            with col3:
                totalPlaylists_df = dataAnalysis.totalPlaylists()
                st.subheader("Total Playlists per Youtube Channels:")
                st.dataframe(totalPlaylists_df, width=500)
            with col4:
                fig = px.pie(totalPlaylists_df, names='Channel Name',
                         values='Total Playlists', hole=0.5)
                fig.update_traces(text=totalPlaylists_df['Channel Name'], textinfo='percent+label',
                                texttemplate='%{percent:.2%}', textposition='outside',
                                textfont=dict(color='white'))
                st.plotly_chart(fig, use_container_width=True)

#class to execute selected queries
class sqlQueries:

    #fucntion to get names of all the videos and their corresponding channels
    def q1_allvideonameChannelname():
        myslq_engine = mysql.connector.connect(user='root', 
                                       password='admin', 
                                       host='localhost', 
                                       port='3306', 
                                       database = 'youtubedb')

        crsr = myslq_engine.cursor()
        
        # SQL query to be executed in the database
        sql_command = """SELECT videos_table.video_name as "Video Name" , 
                        channel_table.channel_name as "Channel Name" FROM videos_table
                        left join channel_table On videos_table.channel_id = channel_table.channel_id;"""
        
        # execute the statement
        crsr.execute(sql_command)
        queryResult = crsr.fetchall()

        #adding result to the dataframe
        i = [i for i in range(1, len(queryResult)+1)]
        df = pd.DataFrame(queryResult, columns=['Video Name', 'Channel Name'], index=i)
        df = df.rename_axis('S.No')
        myslq_engine.close()
        return df

    # to get channels have the most number of videos, and how many videos do they have
    def q2_channelnameTotalvideos():
        myslq_engine = mysql.connector.connect(user='root', 
                                       password='admin', 
                                       host='localhost', 
                                       port='3306', 
                                       database = 'youtubedb')

        crsr = myslq_engine.cursor()
        
        # SQL query to be executed in the database
        sql_command = """SELECT channel_name as "Channel Name", total_videos as "Total Videos"
        FROM youtubedb.channel_table
        order by total_videos desc
        limit 10;"""
        
        # execute the statement
        crsr.execute(sql_command)
        queryResult = crsr.fetchall()

        #adding result to the dataframe
        i = [i for i in range(1, len(queryResult)+1)]
        df = pd.DataFrame(queryResult, columns=['Channel Name', 'Total Videos'], index=i)
        df = df.rename_axis('S.No')
        myslq_engine.close()
        return df
    
    # to get the 10 most viewed videos and their respective channel
    def q3_top10_mostviewedvideos():
        myslq_engine = mysql.connector.connect(user='root', 
                                       password='admin', 
                                       host='localhost', 
                                       port='3306', 
                                       database = 'youtubedb')

        crsr = myslq_engine.cursor()
        
        # SQL query to be executed in the database
        sql_command = """select video_name as "Video Name", view_count as "View Count"
                        from youtubedb.videos_table 
                        ORDER BY view_count desc
                        limit 10;
        """
        
        # execute the statement
        crsr.execute(sql_command)
        queryResult = crsr.fetchall()

        #adding result to the dataframe
        i = [i for i in range(1, len(queryResult)+1)]
        df = pd.DataFrame(queryResult, columns=['Video Name', 'View Count'], index=i)
        df = df.rename_axis('S.No')
        myslq_engine.close()
        return df

    #to get comments count on each video and their corresponding video names
    def q4_totalcomments_perVideo():
        myslq_engine = mysql.connector.connect(user='root', 
                                       password='admin', 
                                       host='localhost', 
                                       port='3306', 
                                       database = 'youtubedb')

        crsr = myslq_engine.cursor()
        
        # SQL query to be executed in the database
        sql_command = """SELECT videos_table.video_name as "Video Name", channel_table.channel_name as "Channel Name",  videos_table.comment_count as "Total Comments"
        FROM videos_table
        left join channel_table On videos_table.channel_id = channel_table.channel_id
        order by videos_table.comment_count desc;
        """
        
        # execute the statement
        crsr.execute(sql_command)
        queryResult = crsr.fetchall()

        #adding result to the dataframe
        i = [i for i in range(1, len(queryResult)+1)]
        df = pd.DataFrame(queryResult, columns=['Video Name', 'Channel Name', 'Total Comments'], index=i)
        df = df.rename_axis('S.No')
        myslq_engine.close()
        return df
    
    # to get videos have the highest number of likes and their  corresponding channel names
    def q5__highestlikes_video():
        myslq_engine = mysql.connector.connect(user='root', 
                                       password='admin', 
                                       host='localhost', 
                                       port='3306', 
                                       database = 'youtubedb')

        crsr = myslq_engine.cursor()
        
        # SQL query to be executed in the database
        sql_command = """SELECT videos_table.video_name as "Video Name", channel_table.channel_name as "Channel Name", videos_table.like_count as "Like Count"
        FROM videos_table
        left join channel_table On videos_table.channel_id = channel_table.channel_id
        order by videos_table.like_count desc
        limit 10;
        """
        
        # execute the statement
        crsr.execute(sql_command)
        queryResult = crsr.fetchall()

        #adding result to the dataframe
        i = [i for i in range(1, len(queryResult)+1)]
        df = pd.DataFrame(queryResult, columns=['Video Name', 'Channel Name', 'Like Count'], index=i)
        df = df.rename_axis('S.No')
        myslq_engine.close()
        return df
    
    # to get total number of likes for each video and their corresponding video names
    def q6_likes_perVideo():
        myslq_engine = mysql.connector.connect(user='root', 
                                       password='admin', 
                                       host='localhost', 
                                       port='3306', 
                                       database = 'youtubedb')

        crsr = myslq_engine.cursor()
        
        # SQL query to be executed in the database
        sql_command = """SELECT videos_table.video_name as "Video Name", videos_table.like_count as "Like Count"
        FROM videos_table
        order by videos_table.like_count desc;
        """
        
        # execute the statement
        crsr.execute(sql_command)
        queryResult = crsr.fetchall()

        #adding result to the dataframe
        i = [i for i in range(1, len(queryResult)+1)]
        df = pd.DataFrame(queryResult, columns=['Channel Name', 'Comment Count'], index=i)
        df = df.rename_axis('S.No')
        myslq_engine.close()
        return df

    # to get the total number of views for each channel and channel names
    def q7_totalviews_perChannel():
        myslq_engine = mysql.connector.connect(user='root', 
                                       password='admin', 
                                       host='localhost', 
                                       port='3306', 
                                       database = 'youtubedb')

        crsr = myslq_engine.cursor()
        
        # SQL query to be executed in the database
        sql_command = """SELECT channel_table.channel_name as "Channel Name", channel_table.channel_views as "Channel View Count"
        FROM Channel_table 
        order by channel_table.channel_views desc;
        """
        
        # execute the statement
        crsr.execute(sql_command)
        queryResult = crsr.fetchall()

        #adding result to the dataframe
        i = [i for i in range(1, len(queryResult)+1)]
        df = pd.DataFrame(queryResult, columns=['Channel Name', 'Channel View Count'], index=i)
        df = df.rename_axis('S.No')
        myslq_engine.close()
        return df

    # to get the names of all the channels that have published videos in the year 2022
    def q8_videos_uploadyear2022():
        myslq_engine = mysql.connector.connect(user='root', 
                                       password='admin', 
                                       host='localhost', 
                                       port='3306', 
                                       database = 'youtubedb')

        crsr = myslq_engine.cursor()
        
        # SQL query to be executed in the database
        sql_command = """SELECT distinct(channel_table.channel_name) as "Channel Name"
        FROM videos_table
        left join channel_table on channel_table.channel_id = videos_table.channel_id 
        where year(videos_table.published_date)= 2023;
        """
        
        # execute the statement
        crsr.execute(sql_command)
        queryResult = crsr.fetchall()

        #adding result to the dataframe
        i = [i for i in range(1, len(queryResult)+1)]
        df = pd.DataFrame(queryResult, columns=['Channel Name'], index=i)
        df = df.rename_axis('S.No')
        myslq_engine.close()
        return df

    # to get average duration of all videos in each channel
    def q9_avgvideoduration_perChannel():
        myslq_engine = mysql.connector.connect(user='root', 
                                       password='admin', 
                                       host='localhost', 
                                       port='3306', 
                                       database = 'youtubedb')

        crsr = myslq_engine.cursor()
        
        # SQL query to be executed in the database
        sql_command = """select channel_table.channel_name as "Channel Name", cast(avg(duration)as time) as "AVG Video Time"
        FROM videos_table
        left join channel_table on videos_table.channel_id = channel_table.channel_id
        group by channel_table.channel_name
        order by  cast(avg(duration)as time) desc;
        """
        
        # execute the statement
        crsr.execute(sql_command)
        queryResult = crsr.fetchall()

        #adding result to the dataframe
        i = [i for i in range(1, len(queryResult)+1)]
        df = pd.DataFrame(queryResult, columns=['Channel Name', 'Avg Video Duration'], index=i)
        df = df.rename_axis('S.No')
        myslq_engine.close()
        df['Avg Video Duration'] = df['Avg Video Duration'].astype(str).str.split(' ').str[-1]
        return df

    # to get videos with most comments and channel names
    def q10_mostcomments_videos():
        myslq_engine = mysql.connector.connect(user='root', 
                                       password='admin', 
                                       host='localhost', 
                                       port='3306', 
                                       database = 'youtubedb')

        crsr = myslq_engine.cursor()
        
        # SQL query to be executed in the database
        sql_command = """select videos_table.video_name as "Video Name", channel_table.channel_name as "Channel Name", videos_table.comment_count as "Comment Count"
        FROM videos_table
        left join channel_table on videos_table.channel_id = channel_table.channel_id
        order by  videos_table.comment_count desc
        limit 10;
        """
        
        # execute the statement
        crsr.execute(sql_command)
        queryResult = crsr.fetchall()

        #adding result to the dataframe
        i = [i for i in range(1, len(queryResult)+1)]
        df = pd.DataFrame(queryResult, columns=['Video Name', 'Channel Name', 'Comment Count'], index=i)
        df = df.rename_axis('S.No')
        myslq_engine.close()
        return df

    def main():
        st.subheader("Select the query you want to execute: ")

        q1 = 'Q1-What are the names of all the videos and their corresponding channels?'
        q2 = 'Q2-Which channels have the most number of videos, and how many videos do they have?'
        q3 = 'Q3-What are the top 10 most viewed videos and their respective channels?'
        q4 = 'Q4-How many comments were made on each video with their corresponding video names?'
        q5 = 'Q5-Which videos have the highest number of likes with their corresponding channel names?'
        q6 = 'Q6-What is the total number of likes for each video with their corresponding video names?'
        q7 = 'Q7-What is the total number of views for each channel with their corresponding channel names?'
        q8 = 'Q8-What are the names of all the channels that have published videos in the 2022 year?'
        q9 = 'Q9-What is the average duration of all videos in each channel with corresponding channel names?'
        q10 = 'Q10-Which videos have the highest number of comments with their corresponding channel names?'

        query_option = st.selectbox(
            '', [q1, q2, q3, q4, q5, q6, q7, q8, q9, q10], 
            index=None,placeholder='Select')

        if query_option == q1:
            st.dataframe(sqlQueries.q1_allvideonameChannelname(), width=1000)

        elif query_option == q2:
            st.dataframe(sqlQueries.q2_channelnameTotalvideos(), width=500)

        elif query_option == q3:
            st.dataframe(sqlQueries.q3_top10_mostviewedvideos(), width=1000)

        elif query_option == q4:
            st.dataframe(sqlQueries.q4_totalcomments_perVideo(), width=1000)

        elif query_option == q5:
            st.dataframe(sqlQueries.q5__highestlikes_video(), width=1000)

        elif query_option == q6:
            st.dataframe(sqlQueries.q6_likes_perVideo(), width=1000)

        elif query_option == q7:
            st.dataframe(sqlQueries.q7_totalviews_perChannel(), width=1000)

        elif query_option == q8:
            st.dataframe(sqlQueries.q8_videos_uploadyear2022(), width=1000)

        elif query_option == q9:
            st.dataframe(sqlQueries.q9_avgvideoduration_perChannel(), width=1000)

        elif query_option == q10:
            st.dataframe(
                sqlQueries.q10_mostcomments_videos(), width=1000)

with st.sidebar:
    image_url = 'https://raw.githubusercontent.com/gopiashokan/Youtube-Data-Harvesting-and-Warehousing/main/youtube_banner.JPG'
    st.image(image_url, use_column_width=True)

    leftnav_option = option_menu(menu_title='', options=['Retrieval/Migration of Data from YouTube API','Migrate Data from MongoDB to SQL', 'Data Analysis', 'SQL Queries', 'Exit'],
                         icons=['youtube', 'database-add', 'database-fill-check', 'list-task', 'pencil-square', 'sign-turn-right-fill'])
if leftnav_option == 'Retrieval/Migration of Data from YouTube API':
    #inputing channel if dron the user
    st.subheader("Please select the options from the drop down as follows: ")
    row_input = st.columns((2,1,2,1))
    with row_input[0]:
        selected_option = st.selectbox(
            'Please select the options from the drop down as follows',
            ('Retrieve Data from Youtube', 'Upload Data to MongoDB'),index=None,placeholder='Select',label_visibility='hidden')

    if selected_option == 'Retrieve Data from Youtube':
        try:
            # get input from user
            row_input = st.columns((2,1,2,1))
            with row_input[0]:
                enteredChannel_id = st.text_input(label='Enter Channel ID',placeholder='Youtube Channel ID')
            submit = st.button(label='Submit')

            if enteredChannel_id != "":
                api_key = 'AIzaSyCViHs3NyBQvncIJpIWhoZClkvwsinw9GQ'
                youtube = build('youtube', 'v3', developerKey=api_key)
                data = {}
                final_data = youtubeExtract.main(enteredChannel_id)
                data.update(final_data)
                channel_name = data['channel_name']['channel_name']

                # display the sample data in streamlit
                #st.json(youtubeExtract.display_sample_data(enteredChannel_id))
                st.success(f'Retrived data from YouTube channel "{channel_name}" successfully')
                st.balloons()
                status = mongodb.tempMongodb(final_data)
        except:
            col1,col2 = st.columns([0.45,0.55])
            with col1:
                st.error("Please enter the valid Channel ID")

    elif selected_option == 'Upload Data to MongoDB':
        status,channel_id = mongodb.toMongodb()
        if status == 'Data collection already exist':
            st.warning("Data collection already exist")
            overwrite_option = st.radio(
                "Do you want to overwrite the collection",
                ["Yes", "No"], index=None)
            if overwrite_option == 'Yes':
                mongodb.drop_collection(channel_id)
                mongodb.droptemp_collection()
                mongodb.toMongodb()
                st.success('Data is overwritten')
                st.balloons()
            elif overwrite_option == 'No':
                mongodb.droptemp_collection()
                st.subheader("Data is not overwritten")

elif leftnav_option == 'Migrate Data from MongoDB to SQL':
    vishwas = MongoClient("mongodb+srv://basotra:yAAipVdBtK8gbRxl@cluster0.nvekqq7.mongodb.net/?retryWrites=true&w=majority")
    db = vishwas['YT_database']
    collection = db.ytExtracted_collection
    total_channels = []
    channel_index = {}
    channel_name_index = 0
    for i in collection.find():
        channel_index[i['channel_name']['channel_name']] = channel_name_index
        total_channels.append(i['channel_name']['channel_name'])
        channel_name_index = channel_name_index + 1
    selected_option = st.selectbox(
        'Please select the channel data you want to migrate to MySQL:',
        (total_channels),index=None,placeholder='Select',label_visibility='hidden')
    if selected_option is not None:
        mysqlData.main(channel_index[selected_option])
        st.success("Data Migrated from MongoDB to SQL")
        st.balloons()

elif leftnav_option == 'Data Analysis':
    dataAnalysis.main()
    
elif leftnav_option == 'SQL Queries':
    sqlQueries.main()

elif leftnav_option == 'Exit':
    mongodb.droptemp_collection()
    st.success('Thank you for your time. Exiting the application')
    st.balloons()
    st.markdown("""
        <meta http-equiv="refresh" content="0; url='https://www.google.com'" />
        """, unsafe_allow_html=True
    )