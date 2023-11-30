from googleapiclient.discovery import build
from pymongo import MongoClient
import pymongo
import psycopg2
import pandas as pd 
import streamlit as st


# api key connection

def Api_connect():
    # Api_Id="AIzaSyCkXCh1juraggfxZbTOYgW9UQgZJcrpm4c"
    #Api_Id ="AIzaSyDll6zfCRVrvK4plRslEAMBxc1Bf51VsJo"
    #Api_Id ="AIzaSyCXj7QOAaS8EiaeuED-Xg57vUxQ1v_4Ic4"
    Api_Id ='AIzaSyCUvlUN0GjD7htQYM4nPr1yCSzrYugi_34'

    api_service_name="youtube"
    api_version="v3"
    
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    return youtube
youtube=Api_connect()   



# get channel information
def get_channel_info(channel_id):
     request = youtube.channels().list(
           part="snippet,contentDetails,statistics",
           id=channel_id
    )
     response = request.execute()
     #response=i
     for i in response['items']:
         data=dict(Channel_Name=i["snippet"]["title"],
                    Channel_Id=i["id"],
                    Subscribers=i ['statistics']['subscriberCount'],
                    Views=i["statistics"]["viewCount"],
                    Total_Videos=i["statistics"]["videoCount"],
                    Channel_description =i["snippet"][ "description"],
                    Playlist_Id=i["contentDetails"][ "relatedPlaylists"][ "uploads"])
     return data


# GET videos id (all videos)
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(
        id = channel_id,
        part='contentDetails'
    ).execute()
    
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids



def get_videos_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response["items"]:
            data = dict(
                channel_Name=item['snippet']['channelTitle'],
                Channel_Id=item['snippet']['channelId'],
                video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=item.get('tags', "N/A"),  
                #Thumbnail=item['snippet']['thumbnails']['default']['url'],
                Description=item.get('description', "N/A"),  
                Published_Date=item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                Views=item['statistics'].get('viewCount', 0),  
                Likes=item['statistics'].get('likeCount', 0), 
                Dislikes=item['statistics'].get('dislikeCount', 0),
                Comments=item['statistics'].get('commentCount', 0), 
                Favourite_Count=item['statistics'].get('favoriteCount', 0),
                Definition=item['contentDetails']['definition'],
                Caption_status=item['contentDetails']['caption']
            )
            video_data.append(data)

    return video_data

# get comment info

def get_comment_info(video_ids):
    Comment_data=[]
    
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()
            for item in response['items']:
                data = dict(
                    comment_Id=item['snippet']['topLevelComment']['id'],
                    video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
        except:
          pass
    return Comment_data



# get_playlist_details

def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:

        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                    Title=item['snippet']['title'],
                    Channal_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    PublishedAt=item['snippet']['publishedAt'],
                    video_Count=item['contentDetails']['itemCount'],)
            All_data.append(data)

        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break 
    return All_data       

# Pymongo connection string

connection_string = "mongodb://localhost:27017"
client = MongoClient(connection_string)
db = client["Youtube_data"]


# MongoDB insert channel

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_videos_ids(channel_id)
    vi_info = get_videos_info(vi_ids)
    com_info = get_comment_info(vi_ids)

    # Insert data into MongoDB
    
    coll1 = db["channel_details"]
    coll1.insert_one({
        "channel_information": ch_details,
        "playlist_information": pl_details,
        "video_information": vi_info,
        "comment_information": com_info
    })

    return "Upload complete"



# Table creation for channels, playlist, videos, Comments
def channels_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="iphone14pro",
        database="Youtube_Data",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    
    create_query = '''
            create table if not exists channels(
                Channel_Name varchar(100),
                Channel_Id varchar(80) primary key,
                Subscribers bigint,
                Views bigint,
                Total_Videos int,
                Channel_description text,
                Playlist_Id varchar(80)
            )
        '''
    cursor.execute(create_query)
    mydb.commit()
    

    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)


        
    for index,row in df.iterrows():
            insert_query = '''
                    insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subscribers,
                                                Views,
                                                Total_Videos,
                                                Channel_description,
                                                Playlist_Id
                                                )
                                            
                                                values(%s,%s,%s,%s,%s,%s,%s)
                                                '''
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Subscribers'],
                    row['Views'],
                    row['Total_Videos'],
                    row['Channel_description'],
                    row['Playlist_Id'])
            
            cursor.execute(insert_query,values)
            mydb.commit()


    
def playlist_table():   
    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="iphone14pro",
                            database="Youtube_Data",
                            port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit() 

    
    create_query='''create table if not exists playlists(Playlist_Id varchar(255) primary key,
                                                        Title varchar(255),
                                                        Channal_Id varchar(255),
                                                        Channel_Name varchar(255),
                                                        PublishedAt timestamp,
                                                        video_Count int
                                                        )'''

    cursor.execute(create_query)
    mydb.commit()


    pl_list=[]
    db=client["Youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
            for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i]) 
    df1=pd.DataFrame(pl_list)


    for index, row in df1.iterrows():

        insert_query = '''
            INSERT into playlists(
                Playlist_Id,
                Title,
                Channal_Id,
                Channel_Name,
                PublishedAt,
                video_Count
                )
                values(%s, %s, %s, %s, %s, %s)'''

        values = (
                    row['Playlist_Id'],
                    row['Title'],
                    row['Channal_Id'],
                    row['Channel_Name'],
                    row['PublishedAt'],
                    row['video_Count'])

        cursor.execute(insert_query, values)
        mydb.commit()




def videos_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="iphone14pro",
        database="Youtube_Data",
        port="5432"
    )
    cursor = mydb.cursor()

    
    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    mydb.commit()

    
    create_query = '''create table if not exists videos(
                    channel_Name varchar(150),
                    Channel_Id varchar(100),
                    video_Id varchar(50) primary key, 
                    Title varchar(150), 
                    Tags text,
                    Description text, 
                    Published_Date timestamp,
                    Duration interval, 
                    Views bigint, 
                    Likes bigint,
                    Dislikes bigint,
                    Comments int,
                    Favourite_Count int, 
                    Definition varchar(10), 
                    Caption_status varchar(50) 
                    )''' 
    
                    
    cursor.execute(create_query)             
    mydb.commit()

    vi_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
            for i in range(len(vi_data["video_information"])):
                vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)


    for index, row in df2.iterrows():
            insert_query = '''INSERT INTO videos(channel_Name,
                                            Channel_Id,
                                            video_Id, 
                                            Title, 
                                            Tags,
                                            Description, 
                                            Published_Date,
                                            Duration, 
                                            Views, 
                                            Likes,
                                            Dislikes,
                                            Comments,
                                            Favourite_Count, 
                                            Definition, 
                                            Caption_status 
                                            )
                                    VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''



                
            values = (
                    row['channel_Name'],
                    row['Channel_Id'],
                    row['video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Dislikes'],
                    row['Comments'],
                    row['Favourite_Count'],
                    row['Definition'],
                    row['Caption_status'])
                                            

            cursor.execute(insert_query,values)
            mydb.commit()



def comments_table():
    
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="iphone14pro",
        database="Youtube_Data",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()

    
    create_query = '''CREATE TABLE if not exists comments(comment_Id varchar(100) primary key,
                    video_Id varchar(80),
                    comment_Text text, 
                    comment_Author varchar(150),
                    comment_Published timestamp)'''
    cursor.execute(create_query)
    mydb.commit()

    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
         for i in range(len(com_data["comment_information"])):
             com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)



    for index, row in df3.iterrows():
        insert_query = '''insert into comments(comment_Id,
                                             video_Id,
                                             comment_Text,
                                             comment_Author,
                                             comment_Published
                                             )
                                        VALUES (%s, %s, %s, %s, %s)'''
  

        values = (
                row['comment_Id'],
                row['video_Id'],
                row['comment_Text'],
                row['comment_Author'],
                row['comment_Published']
                )
                
        cursor.execute(insert_query,values)
        mydb.commit()



def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return"Tables Created successfully"


def show_channels_table():
        ch_list=[]
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
                ch_list.append(ch_data["channel_information"])
        df=st.dataframe(ch_list)

        return df


def show_playlists_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i]) 
    df1=st.dataframe(pl_list)

    return df1


def show_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
            for i in range(len(vi_data["video_information"])):
                vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list)

    return df2


def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
            for i in range(len(com_data["comment_information"])):
                com_list.append(com_data["comment_information"][i])
    df3 = st.dataframe(com_list)

    return df3


with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")
    
channel_id = st.text_input("Enter the Channel id")

if st.button("Collect and Store data"):
    ch_ids = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
    
    if channel_id in ch_ids:
        st.success("Channel details of the given channel id already exists")
    
    else:
        insert = channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="VIDEOS":
    show_videos_table() 

elif show_table=="COMMENTS":
    show_comments_table()       


# SQL CONNECTION

mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="iphone14pro",
        database="Youtube_Data",
        port="5432"
    )
cursor = mydb.cursor()


question=st.selectbox('Please Select Your Question',('1. All the videos and the Channel Name',
                                                     '2. Channels with most number of videos',
                                                     '3. 10 most viewed videos',
                                                     '4. Comments in each video',
                                                     '5. Videos with highest likes',
                                                     '6. likes and Dislikes of all videos',
                                                     '7. views of each channel',
                                                     '8. videos published in the year 2022',
                                                     '9. average duration of all videos in each channel',
                                                     '10. videos with highest number of comments'))

if question=='1. All the videos and the Channel Name':
    queryl='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(queryl)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel_name"])
    st.write(df)

elif question=='2. Channels with most number of videos':
    query2='''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=='3. 10 most viewed videos':
    query3='''select views as views,channel_name as channelname,title as videostitle from videos 
                where views is not null order by views desc limit 10''' 
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)
  

elif question=='4. Comments in each video':
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null''' 
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=='5. Videos with highest likes':
    query5='''select title as videotitle,channel_name as channelname, likes as likecount
                from videos where likes is not null order by likes desc''' 
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=='6. likes and Dislikes of all videos':
    query6='''select video_id, likes, dislikes from videos''' 
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["videotitle","Likes","Dislikes"])
    st.write(df6)

elif question=='7. views of each channel':
    query7='''select channel_name as channelname,views as totalviews from channels''' 
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)    

elif question=='8. videos published in the year 2022':
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos 
                 where extract(year from published_date)=2022''' 
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["video_title","published_date","channel_name"])
    st.write(df8)   


elif question=='9. average duration of all videos in each channel':
    query9='''select channel_name as channelname,AVG(duration) as averegeduration from videos group by channel_name''' 
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channels_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channels_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)     


elif question=='10. videos with highest number of comments':
    query10='''select title as videotitle, channel_name as channelname,comments as comments from videos 
                where comments is not null order by comments desc''' 
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)







