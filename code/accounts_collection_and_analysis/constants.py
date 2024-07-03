import os


class EMAIL_CLIENTS:
    GMAIL = "gmail"
    OUTLOOK = "outlook"


class COLLECTIONS:
    DOMAIN = "domain"
    CRYPTO_ADRESS = "cryptoaddress"
    USER_DETAILS = "twitter_user_details"
    TWITTER_DAILY_DOMAIN_SEARCH = "daily_twitter_domain_search"
    TWITTER_QUEUE_DAILY_DOMAIN_SEARCH = "daily_twitter_username_queue"
    TWITTER_TIMELINESS = "twitter_timelines"
    TWITTER_EXPANDED_URL = "twitter_expanded_url_meta_data"
    PAYPAL_INFO = "paypal_info"
    TELEGRAM_CHANNELS = "telegram_channels"
    DOMAIN_EMAIL_SEARCH = "domain_email_search"
    DOCS_SEARCH = "docs_search"
    YOUTUBE_VIDEO_SEARCH = "youtube_video_search"
    YOUTUBE_APIFY_SEARCH = "youtube_account_search"
    TELEGRAM_APIFY_SEARCH = "telegram_account_search"
    YOUTUBE_META_DATA = "youtube_meta_data"
    INSTAGRAM_ACCOUNT_SEARCH = "instagram_account_search"
    INSTAGRAM_SINGLE_PROFILE_DATA = "instagram_single_profile_data"
    TXT_SIMILARITY = "text_similarity"
    COMBO_SQUATTING_SEQUENCE = "combo_squat_sequence_info"


class ANAMOLY:
    USER_DETAIL_ANAMOLY = "user_detail_anamoly"


# Replace with X/Twitter username suffic
class TWITTER_API_SUFFIX:
    DEFAULT = "DEFAULT"
    ACCT_1 = "ACCT_1"


class SIMILARITY_CLASS:
    COSINE = "cosine"


class ANALYSIS:
    COSINE_SIMILARITY = "username_cosine_similarity"
    PAYPAL_ACCOUNT = "paypal_accounts"
    DOMAIN_CATEGORIES = "domain_categories"
    CANDIDATE_DOMAIN = "candidate_domain"
    DOMAIN_CATEGORIES_COLLATED = "domain_categories_collated"
    DOMAIN_SOCIAL_MEDIA_HANDLE = "domain_social_media_handle"
    DOMAIN_STATUS_CODE = "domain_status_code"
    DATA_COLLECT_TWITTER_HANDLE = "data_collect_twitter_handle"
    DATA_COLLECT_TWITTER_TIMELINES = "data_collect_twitter_timelines"
    DATA_COLLECT_TWITTER_UNWIND_URLS = "data_collect_twitter_unwind_urls"
    DATA_COLLECT_YOU_TUBE_URLS = "data_collect_you_tube_urls"
    TABLE_OVERALL_DATASET = "overall_dataset"
    TLDS = "tlds"


class THIRD_PARTY_APIS:
    KLAZIFY_TOKEN = os.environ['KALIZY_KEY']
    APIFY_TOKEN = os.environ['APIFY_KEY']


class DATA_PATH:
    TWITTER_HANDLE = "brand_impersonation_data/handles/twitter/"
    TWITTER_TIMELINES = "brand_impersonation_data/posts/twitter/"
    TWITTER_UNWIND_URLS = "brand_impersonation_data/urls/twitter/"
    YOURTUBE_URLS = "brand_impersonation_data/youtube_urls/"
