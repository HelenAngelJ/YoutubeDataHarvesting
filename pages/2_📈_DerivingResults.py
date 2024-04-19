import streamlit as st
import pandas as pd
import mysql.connector

@st.cache_resource
def connect_database():
    mydb = mysql.connector.connect(
    host=st.secrets["host"],
    user=st.secrets["user"],
    password=st.secrets["password"],
    database=st.secrets["database"])
    return mydb

def query1():
    connection = connect_database()
    cursor = connection.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select video_name,channel_name from video")
    result1 = cursor.fetchall()
    cursor.close()
    return result1

def query2():
    connection = connect_database()
    cursor = connection.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select Channel_Name,Vedio_count from channel order by Vedio_count desc")
    result2 = cursor.fetchall()
    cursor.close()
    return result2

def query3():
    connection = connect_database()
    cursor = connection.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select video_views,channel_name from video order by video_views desc limit 10")
    result3 = cursor.fetchall()
    cursor.close()
    return result3

def query4():
    connection = connect_database()
    cursor = connection.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select comment_count,video_name from video")
    result4 = cursor.fetchall()
    cursor.close()
    return result4

def query5():
    connection = connect_database()
    cursor = connection.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select like_count,channel_name from video order by like_count desc")
    result5 = cursor.fetchall()
    cursor.close()
    return result5

def query6():
    connection = connect_database()
    cursor = connection.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select like_count,video_name from video")
    result6 = cursor.fetchall()
    cursor.close()
    return result6

def query7():
    connection = connect_database()
    cursor = connection.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("select Channel_Name,Channel_Views from channel")
    result7 = cursor.fetchall()
    cursor.close()
    return result7

def query8():
    connection = connect_database()
    cursor = connection.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("""
    select year(published_date),channel_name
    from video 
    where year(published_date) = 2022
    group by year(published_date),channel_name
    """)
    result8 = cursor.fetchall()
    cursor.close()
    return result8

def query9():
    connection = connect_database()
    cursor = connection.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("""
    select channel_name,round(avg(duration)/60,2)
    from video
    group by channel_name
    """)
    result9 = cursor.fetchall()
    cursor.close()
    return result9

def query10():
    connection = connect_database()
    cursor = connection.cursor()
    cursor.execute(f"use {st.secrets['database']}")
    cursor.execute("""
    select comment_count,channel_name,video_name
    from video
    order by comment_count desc
    """)
    result10 = cursor.fetchall()
    cursor.close()
    return result10

with st.expander(":blue[What are the names of all the videos and their corresponding channels?]"):
    output1 = query1()
    st.table(pd.DataFrame(output1,columns=("Video Name","Channel Name")))

with st.expander(":blue[Which channels have the most number of videos and how many videos do they have?]"):
    output2 = query2()
    df1 = pd.DataFrame(output2,columns=("Channel Name","Number of Videos"))
    st.bar_chart(df1,x="Channel Name",y="Number of Videos")

with st.expander(":blue[What are the top ten most viewed video and their names?]"):
    output3 = query3()
    df2 = pd.DataFrame(output3,columns=("Most Views","Channel Name"))
    st.bar_chart(df2,x ="Most Views" ,y="Channel Name",color="#0000FF")

with st.expander(":blue[How many comments were made on each video, and what are their video names?]"):
    output4 = query4()
    st.table(pd.DataFrame(output4,columns=("Number of Comments","Video Name")))

with st.expander(":blue[Which videos has the highest number of likes and their corresponding video names?]"):
    output5 = query5()
    df3 = pd.DataFrame(output5,columns=("Number of Likes","Channel Name"))
    st.dataframe(df3,hide_index=True)

with st.expander(":blue[What is the total number of likes for each video and what are their corresponding video Name?]"):
    output6 = query6()
    df4 = pd.DataFrame(output6,columns=("Number of Likes","Video Name"))
    st.dataframe(df4,hide_index=True)

with st.expander(":blue[What is the total Number of Views for each Channel?]"):
    output7 = query7()
    df5 = pd.DataFrame(output7,columns=("Channel Name","Total Views"))
    st.dataframe(df5,hide_index=True)

with st.expander(":blue[What are the names of all the channel that have published videos in the year 2022?]"):
    output8 = query8()
    df6 = pd.DataFrame(output8,columns=("Published year","Channel Name"))
    st.dataframe(df6)

with st.expander(":blue[what is the average duration of all videos in each channel and what are their corresponding channel names?]"):
    output9 = query9()
    df7 = pd.DataFrame(output9,columns=("channel Name","Average duration in Minutes"))
    st.dataframe(df7,hide_index=True)

with st.expander(":blue[Which videos have the highest number of comments and what are thier corresponding channel names]"):
    output10 = query10()
    df8 = pd.DataFrame(output10,columns=("Number of comment","Channel Name","Video Name"))
    st.dataframe(df8,hide_index=True)