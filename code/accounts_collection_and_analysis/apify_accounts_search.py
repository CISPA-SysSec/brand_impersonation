import argparse
import os
import shared_util
from constants import COLLECTIONS, THIRD_PARTY_APIS
from apify_client import ApifyClient
from db_util import MongoDBActor


class APIFY_Search:
    def __init__(self, function_name):
        self.function_name = function_name
        self.client = ApifyClient(THIRD_PARTY_APIS.APIFY_TOKEN)

    def process(self):
        if self.function_name == "scrape_instagram_accounts":
            self.scrape_instagram_accounts()
        elif self.function_name == "scrape_instagram_single_profile_data":
            self.scrape_instagram_single_profile_data()
        elif self.function_name == "you_tube_account_search":
            self.scrape_you_tube_accounts()
        elif self.function_name == "telegram_account_posts":
            self.scrape_telegram_account_information()

    def scrape_twitter_accounts(self):
        # Prepare the Actor input
        run_input = {
            "handles": ["Apify"],
            "tweetsDesired": 100,
            "addUserInfo": True,
            "startUrls": [],
            "proxyConfig": {"useApifyProxy": True},
        }

        # Run the Actor and wait for it to finish
        run = self.client.actor("u6ppkMWAx2E2MpEuF").call(run_input=run_input)

        # Fetch and print Actor results from the run's dataset (if there are any)
        for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
            print(item)

    def scrape_instagram_single_profile_data(self):
        _official_domains = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="domain", filter={
            "is_instagram_candidate": True
        }))

        _official_instagram_accounts = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="instagram", filter={
            "is_instagram_candidate": True
        }))

        _len = len(_official_domains)
        _all_usernames = set()
        for cnt, _off_domain in enumerate(_official_domains):
            print("Processing account:{}/{}, {} ".format(cnt, _len, _off_domain))
            if not _off_domain:
                continue
            domain_name = shared_util.get_name_from_domain(_off_domain)
            _found_accounts = shared_util.get_instagram_found_usernames_from_domain_name(domain_name)
            _all_usernames = _all_usernames.union(_found_accounts)

        _all_usernames = _all_usernames.union(_official_instagram_accounts)
        _len_usr = len(_all_usernames)
        for cnt, account_name in enumerate(_all_usernames):
            print("Processing APIFY search:{}/{}, {}".format(cnt, _len_usr, account_name))
            look_up = set(MongoDBActor(COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).distinct(key="username",
                                                                                           filter={
                                                                                               "username": account_name}))

            if len(look_up) > 0:
                print("Already processed escaping the request to APIFY: {}".format(account_name))
                continue
            # Prepare the Actor input
            run_input = {
                "usernames": [account_name]
            }

            try:
                # Run the Actor and wait for it to finish
                run = self.client.actor("apify/instagram-profile-scraper").call(run_input=run_input)

                # Fetch and print Actor results from the run's dataset (if there are any)
                for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                    item['name'] = account_name,
                    MongoDBActor(COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).insert_data(item)
                    print("Data inserted:{}".format(item))
            except Exception as ex:
                print("Exception :{}".format(ex))

    def scrape_instagram_accounts(self):
        _domain_names = shared_util.get_candidate_curated_domains()
        _len = len(_domain_names)
        print(_domain_names)
        for counter, _domain in enumerate(_domain_names):
            _domain_name = shared_util.get_name_from_domain(_domain)
            print("Processing {}/{}, domain:{}, name:{}".format(counter, _len, _domain, _domain_name))
            self.scrape_instagram_accounts_from_domain_name(_domain_name, _domain)

    # https://apify.com/apify/instagram-scraper/input-schema
    def scrape_instagram_accounts_from_domain_name(self, _domain_name, domain):
        # Prepare the Actor input
        run_input = {
            "search": _domain_name,
            "resultsType": "details",
            "resultsLimit": 200,
            "searchType": "hashtag",
            "enhanceUserSearchWithFacebookPage": True,
            "searchLimit": 5,
        }

        try:
            # Run the Actor and wait for it to finish
            run = self.client.actor("apify/instagram-scraper").call(run_input=run_input)

            # Fetch and print Actor results from the run's dataset (if there are any)
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                item['domain'] = domain,
                item['domain_name'] = _domain_name
                MongoDBActor(COLLECTIONS.INSTAGRAM_ACCOUNT_SEARCH).insert_data(item)
        except Exception as ex:
            print("Exception :{}".format(ex))

    def scrape_you_tube_accounts(self):
        _domain_names = shared_util.get_candidate_curated_domains()
        _len = len(_domain_names)
        print(_domain_names)
        for counter, _domain in enumerate(_domain_names):
            _domain_name = shared_util.get_name_from_domain(_domain)
            print("Processing {}/{}, domain:{}, name:{}".format(counter, _len, _domain, _domain_name))
            self.scrape_you_tube_accounts_from_domain(_domain_name, _domain)

    def scrape_you_tube_accounts_from_domain(self, domain_name, _domain):
        run_input = {
            "searchKeywords": domain_name,
            "maxResults": 100,
            "maxResultsShorts": None,
            "maxResultStreams": None,
            "startUrls": [],
            "subtitlesLanguage": "en",
            "subtitlesFormat": "srt",
        }

        try:
            # Run the Actor and wait for it to finish
            run = self.client.actor("h7sDV53CddomktSi5").call(run_input=run_input)

            # Fetch and print Actor results from the run's dataset (if there are any)
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                item['domain'] = _domain,
                item['domain_name'] = domain_name
                MongoDBActor(COLLECTIONS.YOUTUBE_APIFY_SEARCH).insert_data(item)
        except Exception as ex:
            print("Exception :{}".format(ex))

    # this api is used to fetch information from telemetr already fetched data
    def scrape_telegram_account_information(self):
        _telegram_channel = shared_util.get_all_telegram_user_from_db()
        _len = len(_telegram_channel)
        print(_telegram_channel)
        if None in _telegram_channel:
            _telegram_channel.remove(None)
        if "" in _telegram_channel:
            _telegram_channel.remove("")
        for counter, _handle in enumerate(_telegram_channel):
            print("Processing {}/{}, handle:{}".format(counter, _len, _handle))
            self.scrape_each_telegram_account(_handle)
            _check_if_present = set(MongoDBActor(COLLECTIONS.TELEGRAM_APIFY_SEARCH).distinct(key="channelName",
                                                                                             filter={
                                                                                                 "channelName": _handle}))
            if len(_check_if_present) > 0:
                print("Already processed ..{}".format(_handle))
                continue

    def scrape_each_telegram_account(self, _account):
        # Prepare the Actor input
        run_input = {
            "channels": [_account],
            "postsFrom": 1,
            "postsTo": 200,
            "proxy": {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"],
            },
        }

        try:
            # Run the Actor and wait for it to finish
            run = self.client.actor("73JZk4CeKcDsWoJQu").call(run_input=run_input)

            # Fetch and print Actor results from the run's dataset (if there are any)
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                MongoDBActor(COLLECTIONS.TELEGRAM_APIFY_SEARCH).insert_data(item)
        except Exception as ex:
            print("Exception :{}".format(ex))


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="APIFY API Search")
    _arg_parser.add_argument("-p", "--process_name",
                             action="store",
                             required=True,
                             help="processing function name")

    _arg_value = _arg_parser.parse_args()

    meta_data = APIFY_Search(_arg_value.process_name)
    meta_data.process()
