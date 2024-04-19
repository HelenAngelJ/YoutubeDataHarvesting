import streamlit as st
import googleapiclient.discovery
import pandas as pd

#api connection
@st.cache_resource
def api_connection():
    api_service_name = st.secrets["api_service_name"]
    api_version = st.secrets["api_version"]
    api_key = st.secrets["api_key"]
    youtube = googleapiclient.discovery.build(api_service_name, api_version,developerKey=api_key)
    return youtube

#getting channel data
@st.cache_data(show_spinner=False)
def channel_Data(channel_id):
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
    
    channel_df = pd.DataFrame.from_dict(channel_data)

    return channel_df

#initializing session state for scrap_button
def button(button_name):
    if button_name not in st.session_state:
        st.session_state[button_name] = False

def click_button(button_name):
    st.session_state[button_name] = True

#the scrap details page
st.title(":blue[YouTube Data Harvesting]")

Channel_id = st.text_input("enter channel ID",key="channel_id")
button("scrap_button")
scrap_button = st.button(":blue[get Channel Details]",on_click=click_button("scrap_button"))
if scrap_button:
    channel_dataframe = channel_Data(st.session_state.channel_id)
    st.dataframe(channel_dataframe)
    st.page_link("pages/1_📊_DataFrame_view.py", label=":blue[Go to Dataframe view]")

    