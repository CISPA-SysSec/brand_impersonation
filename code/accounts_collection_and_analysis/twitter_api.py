import json
import os
import random

import shutil
import requests
import time

import tweepy

from constants import ANAMOLY, TWITTER_API_SUFFIX
from db_util import MongoDBActor


class TwitterFeeds:
    def __init__(self, search_param=None, twitter_cred=1):
        self.twitter_cred = twitter_cred
        self.search_param = search_param
        self.search_url = "https://api.twitter.com/1.1/users/search.json"
        self.query_params = self.create_query_params()
        self.graceful_wait = 2

    def sleep_in_too_many_requests(self, msg):
        if 'Too Many Requests' in msg:
            print("Found too many requests exception.. sleeping for a minute ..")
            time.sleep(60)

    def get_random_bearer_token(self):
        tokens = [
            os.environ['TWITTER_BEARER_TOKEN_ACC_1'],
            os.environ['TWITTER_BEARER_TOKEN_DEFAULT']
        ]
        return random.choice(tokens)

    def create_query_for_exact_match_user_name(self):
        fields = "created_at,description"
        params = {"usernames": self.search_param, "user.fields": fields}
        return params

    def create_query_params(self):
        return {
            'q': self.search_param,
            'page': 40,
            'count': 20
        }

    def bearer_oauth(self, r):
        r.headers[
            "Authorization"] = f"Bearer {'{}'}".format(self.get_random_bearer_token())
        r.headers["User-Agent"] = "v2RecentSearchPython"
        return r

    def make_get_request(self, url, params):
        response = requests.get(url, auth=self.bearer_oauth, params=params)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()

    def main(self):
        json_response = self.make_get_request(self.search_url, self.query_params)
        print(json.dumps(json_response, indent=4, sort_keys=True))
        while True:
            if "meta" in json_response:
                if "next_token" in json_response['meta']:
                    next_token = json_response["meta"]["next_token"]
                    self.query_params['next_token'] = next_token
                    json_response = self.make_get_request(self.search_url, self.query_params)
                    json_response['search_text'] = self.search_param
                    time.sleep(self.graceful_wait)
                else:
                    break
            else:
                break

    def get_user_fields(self):
        return {
            "tweet.fields": "attachments,author_id,context_annotations,conversation_id,created_at,edit_controls,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,reply_settings,source,text,withheld",
            "count": 199,
            "skip_status": True,
            "include_user_entities": False,
            "screen_name": self.search_param
        }

    def get_followers(self):
        _all_followers = set()
        _all_json = []
        try:
            url = "https://api.twitter.com/1.1/followers/list.json"
            params = self.get_user_fields()
            json_response = self.make_get_request(url, params)
            print(json_response)
            _all_json = [json_response]
            while True:
                if "next_cursor" in json_response:
                    next_token = json_response["next_cursor"]
                    params['next_cursor'] = next_token
                    json_response = self.make_get_request(url, params)
                    print(json_response)
                    _all_json = _all_json + [json_response]
                    time.sleep(self.graceful_wait)
                else:
                    break
        except Exception as ex:
            print(str(ex))
            self.sleep_in_too_many_requests(str(ex))

        for _json in _all_json:
            if 'users' in _json:
                _users = _json['users']
                for user in _users:
                    if 'screen_name' in user:
                        _all_followers.add(user['screen_name'])

        return list(_all_followers)

    # search the user name by the provided search matching
    # Ex. Search phrase such as "bitcoin support" searches the user by the matching keyword
    # API Reference
    # https://developer.twitter.com/en/docs/twitter-api/v1/accounts-and-users/follow-search-get-users/api-reference/get-users-search
    # 900 request per 15 minutes
    # 1 request per second
    def fetch_user_name_v1_api(self):
        search_url = "https://api.twitter.com/1.1/users/search.json"
        pagination_count = 1
        do_continue = True
        all_found_screen_names = set()
        while do_continue:
            try:
                previous_total_account = len(all_found_screen_names)
                query_param = {'q': self.search_param, 'page': pagination_count, 'count': 20}
                json_response = self.make_get_request(search_url, query_param)
                _found_response_screen_name = set()
                _pagination_data = set()
                for _data in json_response:
                    if 'screen_name' in _data:
                        all_found_screen_names.add(_data['screen_name'])

                new_account_count = len(all_found_screen_names)
                if new_account_count == previous_total_account:
                    print("Breaking from pagination {}, no new result found".format(pagination_count))
                    break

                time.sleep(1.25)
            except Exception as ex:
                print("Exception occurred {}".format(ex))
                self.sleep_in_too_many_requests(str(ex))
                do_continue = False
            pagination_count += 1
        return list(all_found_screen_names)

    def fetch_user_detail_by_screen_name(self):
        try:
            search_url = "https://api.twitter.com/2/users/by"
            fields = "created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url," \
                     "protected,public_metrics,url,username,verified,withheld"
            query_param = {"usernames": self.search_param,
                           "user.fields": fields
                           }

            json_response = self.make_get_request(search_url, query_param)
            return json_response
        except Exception as ex:
            print("Exception occurred in fetching Twitter account data {}".format(ex))
            _id = MongoDBActor(ANAMOLY.USER_DETAIL_ANAMOLY).find_and_modify(
                key={'screen_name': self.search_param},
                data={
                    'screen_name': self.search_param,
                    'detail': "{}".format(ex),

                })
            print("Inserted with exception {}, upsert_id:{}".format(self.search_param, _id))

            self.sleep_in_too_many_requests(str(ex))

    def fetch_user_detail_by_user_id(self, numeric_user_id):
        try:
            search_url = "https://api.twitter.com/2/users/{}".format(numeric_user_id)
            fields = "created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url," \
                     "protected,public_metrics,url,username,verified,withheld"
            query_param = {
                "user.fields": fields
            }

            json_response = self.make_get_request(search_url, query_param)
            return json_response
        except Exception as ex:
            print("Exception occurred in fetching Twitter account data {}".format(ex))
            _id = MongoDBActor(ANAMOLY.USER_DETAIL_ANAMOLY).find_and_modify(
                key={'user_id': numeric_user_id},
                data={
                    'user_id': numeric_user_id,
                    'detail': "{}".format(ex),

                })
            print("Inserted with exception {}, upsert_id:{}".format(self.search_param, _id))

            self.sleep_in_too_many_requests(str(ex))

    def download_image(self, request_url, save_path):
        try:
            response = requests.get(request_url,
                                    auth=self.bearer_oauth,
                                    stream=True)
            if response.status_code != 200:
                raise Exception(response.status_code, response.text)

            if response.status_code == 200:
                with open('{}'.format(save_path), 'wb') as f:
                    response.raw.decode_content = True
                    shutil.copyfileobj(response.raw, f)
        except Exception as ex:
            print("Exception occurred in twitter request to image {}".format(ex))
            self.sleep_in_too_many_requests(str(ex))

    """
        Input:
            numeric_user_id
            additional_query_param: list of key, pair 
                The additional query param can be formatted based on supported query param: https://developer.twitter.com/en/docs/twitter-api/tweets/timelines/api-reference/get-users-id-tweets

        output: The output will be either errors or list of tweets data found
            list of errors  
                OR
            list of data 
    """
    def get_user_tweets(self, numeric_user_id='1532420332497342466', _additional_query_param=None):
        _tweets_data = []
        _url = "https://api.twitter.com/2/users/{}/tweets/".format(numeric_user_id)
        _param = {
            'tweet.fields': 'attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,'
                            'id,in_reply_to_user_id,lang,possibly_sensitive,referenced_tweets,reply_settings,'
                            'source,text,withheld',
            'expansions': 'attachments.poll_ids,attachments.media_keys,author_id,geo.place_id,in_reply_to_user_id,'
                          'referenced_tweets.id,entities.mentions.username,referenced_tweets.id.author_id',
            'place.fields': 'contained_within,country,country_code,full_name,geo,id,name,place_type',
            'user.fields': 'created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld',
            'media.fields': 'duration_ms,height,media_key,preview_image_url,type,url,width,public_metrics,alt_text,variants',
            'max_results': '100'
        }

        if _additional_query_param:
            if type(_additional_query_param) is dict:
                for k, v in _additional_query_param.items():
                    _param[k] = v

        try:
            json_response = self.make_get_request(_url, _param)

            if 'errors' in json_response:
                _errors = json_response['errors']
                return _errors

            if 'data' not in json_response:
                return []

            _data = json_response['data']
            for _tweet in _data:
                _tweets_data.append(_tweet)

            # error case where user_id is suspended
            """
                {
                    "errors": [
                        {
                            "detail": "User has been suspended: [2881117795].",
                            "parameter": "id",
                            "resource_id": "2881117795",
                            "resource_type": "user",
                            "title": "Forbidden",
                            "type": "https://api.twitter.com/2/problems/resource-not-found",
                            "value": "2881117795"
                        }
                    ]
                }

            """

            # returning the data
            """
                {
                    "data": [

                        { .... },
                        { .... }
                        ....                    
                    ],
                    "meta": {
                        "newest_id": "1592974543773396993",
                        "next_token": "7140dibdnow9c7btw424c28rstvlbelekdc8wj65ji4vs",
                        "oldest_id": "1592600829760094208",
                        "result_count": 100
                    }
                }

            """
            counter = 1
            while True:
                if "meta" in json_response:
                    if "next_token" in json_response['meta']:
                        next_token = json_response["meta"]["next_token"]
                        _param['pagination_token'] = next_token
                        json_response = self.make_get_request(_url, _param)

                        if 'data' not in json_response:
                            continue
                        _data = json_response['data']
                        for _tweet in _data:
                            _tweets_data.append(_tweet)
                        time.sleep(self.graceful_wait)
                        print("user_id:{}, Tweet pagination request count:{}".format(numeric_user_id, counter))
                        counter = counter + 1
                    else:
                        break
                else:
                    break
        except Exception as ex:
            print("Exception occurred: {}, numeric_id:{}".format(ex, numeric_user_id))
            self.sleep_in_too_many_requests(str(ex))
        return _tweets_data

    # Basic API does not support full search and only supports recent search
    # Use the search type based on the API availability
    def get_full_search_text_tweets_info(self, search_text, _additional_query_param=None, search_type='recent'):
        _tweets_data = []
        if search_type == 'recent':
            _url = "https://api.twitter.com/2/tweets/search/recent"
        elif search_type == 'full':
            _url = "https://api.twitter.com/2/tweets/search/all"
        else:
            raise Exception("unsupported search type {}".format(search_type))

        _param = {
            "query": search_text,
            'tweet.fields': 'attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,'
                            'id,in_reply_to_user_id,lang,possibly_sensitive,referenced_tweets,reply_settings,'
                            'source,text,withheld',
            'expansions': 'attachments.poll_ids,attachments.media_keys,author_id,geo.place_id,in_reply_to_user_id,'
                          'referenced_tweets.id,entities.mentions.username,referenced_tweets.id.author_id',
            'place.fields': 'contained_within,country,country_code,full_name,geo,id,name,place_type',
            'user.fields': 'created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld',
            'media.fields': 'duration_ms,height,media_key,preview_image_url,type,url,width,public_metrics,alt_text,variants',
            'max_results': '100'
        }

        if _additional_query_param:
            if type(_additional_query_param) is dict:
                for k, v in _additional_query_param.items():
                    _param[k] = v

        try:
            json_response = self.make_get_request(_url, _param)

            print(json_response)
            time.sleep(5)

            if 'errors' in json_response:
                # this is list
                _errors = json_response['errors']
                return _errors

            if 'data' not in json_response:
                return []

            _data = json_response['data']
            for _tweet in _data:
                _tweets_data.append(_tweet)

            counter = 1
            while True:
                if "meta" in json_response:
                    if "next_token" in json_response['meta']:
                        next_token = json_response["meta"]["next_token"]
                        _param['pagination_token'] = next_token
                        json_response = self.make_get_request(_url, _param)

                        print(json_response)
                        if 'data' not in json_response:
                            continue
                        _data = json_response['data']

                        for _tweet in _data:
                            _tweets_data.append(_tweet)
                        time.sleep(self.graceful_wait + 1)
                        print("search_text:{}, Tweet pagination request count:{}".format(search_text, counter))
                        counter = counter + 1

                        # 15 min wait, API Limit surpass
                        if counter == 57:
                            time.sleep(15 * 60)

                    else:
                        break
                else:
                    break
        except Exception as ex:
            self.sleep_in_too_many_requests(str(ex))
            print("Exception occurred: {}, search_text:{}".format(ex, search_text))
            return "{}".format(ex)
        return _tweets_data

    def bulk_user_look_up(self, lst):
        _random_suffix = random.choice([
            TWITTER_API_SUFFIX.DEFAULT,
            TWITTER_API_SUFFIX.ACCT_1
        ])

        api_key = os.environ["TWITTER_API_KEY_{}".format(_random_suffix)]
        api_secrets = os.environ["TWITTER_API_SECRET_{}".format(_random_suffix)]
        access_token = os.environ["TWITTER_ACCESS_TOKEN_{}".format(_random_suffix)]
        access_secret = os.environ["TWITTER_ACCESS_TOKEN_SECRET_{}".format(_random_suffix)]

        auth = tweepy.OAuthHandler(api_key, api_secrets)
        auth.set_access_token(access_token, access_secret)

        try:
            api = tweepy.API(auth, wait_on_rate_limit=True)
            response = api.lookup_users(screen_name=lst)
            _returned_data = []
            for val in response:
                _user_detail = val._json
                _returned_data.append(_user_detail)
            return _returned_data

        except Exception as ex:
            print("Exception occurred {}".format(ex))
            self.sleep_in_too_many_requests(str(ex))
        return []

    def post_tweets(self, status, _account=3):
        try:
            if _account == 1:
                _suffix = TWITTER_API_SUFFIX.ACCT_1
            else:
                # DO NOT USE DEFAULT
                _suffix = TWITTER_API_SUFFIX.DEFAULT

            api_key = os.environ["TWITTER_API_KEY_{}".format(_suffix)]
            api_secrets = os.environ["TWITTER_API_SECRET_{}".format(_suffix)]
            access_token = os.environ["TWITTER_ACCESS_TOKEN_{}".format(_suffix)]
            access_secret = os.environ["TWITTER_ACCESS_TOKEN_SECRET_{}".format(_suffix)]

            auth = tweepy.OAuthHandler(api_key, api_secrets)
            auth.set_access_token(access_token, access_secret)

            api = tweepy.API(auth, wait_on_rate_limit=True)
            response = api.update_status(status=status)
            return True, response._json

        except Exception as ex:
            msg = str(ex)
            return False, msg
