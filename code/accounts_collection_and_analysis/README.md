##  Brand Impersonation: Social Media Profiles Data Collection and Analysis Readme File
## Dependencies and Installation

Please refer to Pipfile for dependencies required for code usage. The code deployed used python version 3.9.6.

The data storage is expected to install MongoDB for any storage or processing.

## Collecting Raw Dataset

`` Pre-requisite ``
Prior to running the below code, there are two pre-requisites:

* `Installation of MongoDB`: The data are stored using MongoDB database. We suggest installing MongoDB as per your system requirement as instructed by MongoDB installation guide [link](https://www.mongodb.com/docs/manual/administration/install-community/) 

* `External API Setup`: The code uses three external services: APIFY [API](https://apify.com/), Klazify [API](https://www.klazify.com/), X Platform [API](https://developer.x.com/en/docs/twitter-api). These API are added as part of environment variable via `constants.py` at `class THIRD_PARTY_APIS` and at `class TWITTER_API_SUFFIX`.

After installation of MongoDB and external API setup, following are the commands for performing data collection from various social media platforms.

1. To create raw dataset based on keywords - Finding potential impersonation attacks

    ```
    python apify_accounts_search.py (third party data collection API call)
    python telegram_account_search.py (in-house selenium)
    python twitter_accounts_search.py (twitter official API)
    python you_tube_account_search.py (third party YouTube API)
    ```
2. To collect profile metadata - Finding profile data related to the given social media profile
    
    ```
    python account_meta_data_collect.py
    ```

3. To collect domain name (ex. paypal.com) associated social media profiles

    ```
    python scrape_website_social_media.py
    ```
4. To collect domain name (ex. paypal.com) associated web categories (third party APIs)

    ```
    python web_categories_api.py
    ```

## Squatting Analysis 

The following are commands for performing data analysis related to squatting handles.

1. Pre-processing of the collected data for analysis (see file content for datapoint evaluation)  
    ```
    python data_creator.py 
    ```

2. After pre-processing, analyze the data for various datapoints

    ```
    python analysis.py
    ```

## Disclosure 
Additionally, we perform data disclosure via running following command to given platform.

    ```
    python disclosure.py (platform)
    ```