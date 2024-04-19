import streamlit as st
import googleapiclient.discovery
import pandas as pd
import mysql.connector

try:
    @st.cache_resource
    def api_connection():
        api_service_name = st.secrets["api_service_name"]
        api_version = st.secrets["api_version"]
        api_key = st.secrets["api_key"]
        youtube = googleapiclient.discovery.build(api_service_name, api_version,developerKey=api_key)
        return youtube

    @st.cache_data(show_spinner=False)
    def channel_Data(channel_id):
        st.session_state.channel_id = channel_id
        connection = api_connection()
        request = connection.channels().list(
        part="snippet,contentDetails,statistics,localizations,status,topicDetails",
        id=channel_id)
        response = request.execute()

        # getting the data in the form of dictionary
        channel_data = [dict(Channel_ID=response['items'][0]['id'],
                Channel_Name = response['items'][0]['snippet']['title'],
                Channel_Views = int(response['items'][0]['statistics']['viewCount']),
                Channel_Description = response['items'][0]['snippet']['description'],
                Channel_Status = response['items'][0]['status']['privacyStatus'],
                Subscriber_Count = int(response['items'][0]['statistics']['subscriberCount']),
                Vedio_count = int(response['items'][0]['statistics']['videoCount']),
                Playlist_ID = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])]

        return channel_data

    #getting vedio detials
    @st.cache_data(show_spinner=False)
    def getting_vedio_data(Id):
        video_ids=[]
        channel_data = Id
        Playlist_Id=channel_data[0]['Playlist_ID']
        next_page_token=None
        while True:
            youtube = api_connection()
            response = youtube.playlistItems().list(
            part="snippet",
            maxResults=50,
            pageToken=next_page_token,
            playlistId=Playlist_Id).execute()
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=response.get('nextPageToken')
            if next_page_token is None:
                break
        return video_ids

    @st.cache_data(show_spinner=False)
    def duration_convert(duration):
        duration = duration.removeprefix("PT")
        time = 0
        if ("H" in duration) and ("M" in duration) and ("S" in duration):
            times = duration.split("H")
            hour = int(times[0]) *3600
            remain = times[-1]
            if "M" != remain[-1]:
                remain = remain.split("M")
                minute = int(remain[0]) * 60
                if "S" in remain[1]:
                    seconds = remain[1].removesuffix("S")
                    seconds = int(seconds)
                    time = minute + seconds
            elif remain[-1] != "S":
                minutes = remain.removesuffix("M")
                time = hour + (int(minutes) * 60)
            else:
                seconds = remain.removesuffix("M")
                time = hour + int(seconds)
        elif ("H" not in duration) and ("M" in duration) and ("S" in duration):
            remain = duration.split("M")
            minute = int(remain[0]) * 60
            seconds = remain[1].removesuffix("S")
            seconds = int(seconds)
            time = minute + seconds
        if ("M" == duration[-1]) and ("H" not in duration) and ("S" not in duration):
            duration = duration.removesuffix("M")
            time = int(duration) * 60
        if ("S" == duration[-1]) and ("H" not in duration) and ("M" not in duration):
            duration = duration.removesuffix("S")
            time = int(duration)
        if ("H" == duration[-1]) and ("M" not in duration) and ("S" not in duration):
            duration = duration.removesuffix("H")
            time = int(duration) * 3600
        
            

        return time


    @st.cache_data(show_spinner=False)
    def date_convert(publish_date):
        publish_date = publish_date.removesuffix("Z")
        publish_date = publish_date.replace("T"," ")
        return publish_date

    @st.cache_data(show_spinner=False)
    def get_video_info(video_ids):
        video_data=[]
        for video_id in video_ids:
            youtube = api_connection()
            request=youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )
            response=request.execute()
            
            for item in response["items"]:
                data=dict(channe_name=item['snippet']['channelTitle'],
                            channel_id=item['snippet']['channelId'],
                            video_Id=item['id'],
                            title=item['snippet']['localized']['title'],
                            description=item['snippet']['localized']['description'],
                            published_at = date_convert(item['snippet']['publishedAt']),
                            views=item['statistics']['viewCount'],
                            like_count=item['statistics']['likeCount'],
                            favourite_count = int(item['statistics']['favoriteCount']),
                            comments=item['statistics']['commentCount'],
                            duration =duration_convert(item['contentDetails']['duration']),
                            caption_status=item['contentDetails']['caption']
                            )
                video_data.append(data)
        #vedio_dataframe = pd.DataFrame.from_dict(video_data)
        return video_data

    #getting comments data
    @st.cache_data(show_spinner=False)
    def getting_comment_data(video_id_list):
        comment_data=[]
        for each_video_id in video_id_list:
            try:
                youtube = api_connection()
                request = youtube.commentThreads().list(
                    part="snippet",
                    videoId = each_video_id,
                    maxResults = 50
                )
                response2 = request.execute()
                for each_item in response2['items']:
                    each_comment_data = dict(comment_id=each_item['snippet']['topLevelComment']['id'],
                                        video_id = each_item['snippet']['topLevelComment']['snippet']['videoId'],
                                        comment_text=each_item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                        comment_author=each_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                        comment_publish_date=date_convert(each_item['snippet']['topLevelComment']['snippet']['publishedAt']))
                    comment_data.append(each_comment_data)
            except :
                continue
        #comment_dataframe = pd.DataFrame.from_dict(comment_data)
        return comment_data

    @st.cache_resource
    def connect_database():
        mydb = mysql.connector.connect(
        host=st.secrets["host"],
        user=st.secrets["user"],
        password=st.secrets["password"],
        database=st.secrets["database"])
        return mydb

    def check_table_exists(table_name):
        connection = connect_database()
        cursor = connection.cursor()
        cursor.execute("show tables")
        result = cursor.fetchall()
        cursor.close()
        exist = 0
        for i in result:
            if table_name in i:
                exist = 1
                break
        return exist

    def create_table(table_name):
        connection = connect_database()
        cursor = connection.cursor()
        cursor.execute(f"use {st.secrets['database']}")
        if table_name == "channel":    
            cursor.execute("""
            create table if not exists channel (
            Channel_ID varchar(255) not null, 
            Channel_Name char(150) not null,
            Channel_Views int,
            Channel_Description longtext,
            Channel_Status varchar(20),
            Subscriber_Count int,
            Vedio_count int,
            primary key(Channel_ID)
            )
            """)
            connection.commit()
        elif table_name == "video":
            cursor.execute("""
            create table if not exists video (
            channel_name varchar(255),
            channel_id varchar(255) not null,
            video_id varchar(255) unique not null, 
            video_name varchar(200),
            video_description longtext,
            published_date datetime,
            video_views int,
            like_count int,
            favourite_count int,
            comment_count int,
            duration int,
            caption_status varchar(20),
            primary key(video_id),
            foreign key (channel_id) references channel(channel_id)
            )
            """)
            connection.commit()
        elif table_name == "comment":
            cursor.execute("""
            create table if not exists comment(
            comment_id varchar(255) unique not null,
            comment_video_id varchar(255) not null,
            comment_text longtext,
            comment_author varchar(150),
            comment_published_date datetime,
            primary key(comment_id),
            foreign key(comment_video_id) references video(video_id)
            )
            """)
            connection.commit()
        cursor.close()

    def insert_details(table_name,value):
        connection = connect_database()
        cursor = connection.cursor()
        pass_value = []
        for i in value:
            lst = []
            for j in i.values():
                lst.append(j)
            lst = tuple(lst)
            pass_value.append(lst)

        str_form = ""
        for u in range(0,len(pass_value)):
            if u != len(pass_value)-1 :
                str_form += str(pass_value[u])+","
            else:
                str_form += str(pass_value[u])

        #st.write(str_form)
        if table_name == "channel":
            try:
                cursor.execute(f"""
            insert into channel (Channel_ID,Channel_Name,Channel_Views,Channel_Description,Channel_Status,Subscriber_Count,Vedio_count) values {str_form}
            """)
                connection.commit()
                st.success("channel data inserted")
            except Exception as err:
                st.write(err)
            
        elif table_name == "video":
            try:
                cursor.execute(f"""
            insert into video (channel_name,channel_id,video_Id,video_name,video_description,published_date,video_views,like_count,favourite_count,comment_count,duration,caption_status) values {str_form}
            """)
                connection.commit()
                st.success("vedio data inserted")
            except Exception as err:
                st.write(err)
            
        elif table_name == "comment":
            try:
                cursor.execute(f"""
            insert into comment (comment_id,comment_video_id,comment_text,comment_author,comment_published_date) values {str_form}
            """)
                connection.commit()
                st.success("comment data inserted")
            except Exception as err:
                st.write(err)
            

    #initializing session state for db_button
    def button(button_name):
        if button_name not in st.session_state:
            st.session_state[button_name] = False

    def click_button(button_name):
        st.session_state[button_name] = True


    Channel_id = st.session_state.channel_id
    Chnl_data = channel_Data(Channel_id)
    st.session_state.channel_id = Channel_id
    vedio_ids = getting_vedio_data(Chnl_data)
    Vedio_dataframe = get_video_info(vedio_ids)
    Comment_data = getting_comment_data(vedio_ids)
    db_button = button("db")
    video_button = button("video_db")
    comment_button = button("comment_db")

    col1,col2,col3 = st.columns(3)

    choose = st.radio("select to view details",["Channel Details","Vedio Details","Comment Details"])

    if choose == "Channel Details":
        st.subheader(f":blue[{Chnl_data[0]['Channel_Name']} Channel details]")
        with st.expander(":blue[view channel detials]"):
            st.table(Chnl_data)
        db_button = st.button(":blue[move "+Chnl_data[0]['Channel_Name']+" Channel details to Database]",on_click=click_button("db"))

    
    elif choose == "Vedio Details":
        st.subheader(f":blue[{Chnl_data[0]['Channel_Name']} video details]")
        with st.expander(f":blue[view {len(Vedio_dataframe)} video detials]"):
            st.table(pd.DataFrame.from_dict(Vedio_dataframe))
        video_button = st.button(":blue[move "+Chnl_data[0]["Channel_Name"]+" video details to Database]",on_click=click_button("video_db"))
    else:
        st.subheader(f":blue[{Chnl_data[0]['Channel_Name']} comment details]")
        with st.expander(f":blue[view {len(Comment_data)} comment details]"):
            st.table(pd.DataFrame.from_dict(Comment_data))
        comment_button = st.button(":blue[move "+Chnl_data[0]["Channel_Name"]+" comment details to Database]",on_click=click_button("comment_db"))

    with col1:    
        if db_button:
            result = check_table_exists("channel")
            del Chnl_data[0]["Playlist_ID"]
            if result == 1:
                insert_details("channel",Chnl_data)
            else:
                create_table("channel")
                insert_details("channel",Chnl_data)

    with col2:
        if video_button:
            result = check_table_exists("video")
            if result == 1:
                insert_details("video",Vedio_dataframe)
            else:
                create_table("video")
                insert_details("video",Vedio_dataframe)
    with col3:
        if comment_button:
            result = check_table_exists("comment")
            if result == 1:
                insert_details("comment",Comment_data)
            else:
                create_table("comment")
                insert_details("comment",Comment_data)

    with st.sidebar:
        st.page_link("pages/2_📈_DerivingResults.py", label=":blue[Go to Results Page]")
except (AttributeError,NameError,KeyError) as err:
    st.error("No channel Id provided")
    st.page_link("YoutubeDataHarvestingapp.py", label=":blue[YouTubeDataHarvestingapp]")


