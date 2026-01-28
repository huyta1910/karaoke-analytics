import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    FB_PAGE_ID = os.getenv('FB_PAGE_ID')
    FB_ACCESS_TOKEN = os.getenv('FB_ACCESS_TOKEN')
    
    GOOGLE_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
    BQ_DATASET_ID = os.getenv('BQ_DATASET_ID')
    
    BQ_TABLE_ID = os.getenv('BQ_TABLE_ID')       
    BQ_POSTS_TABLE_ID = "post_performance"         

    API_VERSION = "v19.0"
    DATE_CHUNK_SIZE_DAYS = 90
    
    METRICS = [
        'page_post_engagements',
        'page_views_total',
        'page_video_views',
        'page_actions_post_reactions_like_total'
    ]