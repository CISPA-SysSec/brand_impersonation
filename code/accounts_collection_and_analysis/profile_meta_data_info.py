import argparse
import json

import db_util
import shared_util
import constants
import validators
from spacy.lang.en import English
nlp = English(entity=True)


class ProfileMetaData:
    def __init__(self, function):
        self.function = function
        self.officials = self.all_official()
        self.web_category_official = self._categorical_official_data()

    def process(self):
        if self.function == "location_info":
            self.location_info()
        elif self.function == "profile_description_urls":
            self.profile_description_urls()
        elif self.function == "account_creation_date":
            self.account_creation_date()
        elif self.function == "human_info":
            self.human_name_info()
        elif self.function == "blocked_account":
            self.blocked_account()
        elif self.function == "followers_metric":
            self.followers_metrics()
        elif self.function == "posts_metric":
            self.post_metrics()
        elif self.function == "found_email_brand_target":
            self.found_email_brand_target()
        elif self.function == "overall_social_media_based_target":
            self.overall_social_media_based_target()
        elif self.function == "brands_targeted_count":
            self.brands_targeted_count()
        elif self.function == "web_category_count":
            self.web_category_count()
        elif self.function == "target_30_domain_and_rank_printer":
            self.target_30_domain_and_rank_printer()
        elif self.function == "fetch_twitter_account_details_for_remaining_search_accounts_users":
            self.fetch_twitter_account_details_for_remaining_search_accounts_users()
        elif self.function == "image_cluster_top_30_brands":
            self.image_cluster_top_30_brands()
        elif self.function == "posts_cluster_analysis":
            self.posts_cluster_analysis()

    def posts_cluster_analysis(self):
        _dir_path = "brand_impersonation/report//data_clustering_username_squat"
        _cluster_path = "posts_cluster_results"
        _result_path = "results_summary.csv"
        with open("{}/{}/{}".format(_dir_path, _cluster_path, _result_path), "r") as f_read:
            lines = f_read.readlines()

        _clusters = {}
        _handles_results = "results_filtered_list_of_handles.csv"
        full_path = "{}/{}/{}".format(_dir_path, _cluster_path, _handles_results)

        with open(full_path, "r") as f_read:
            lines = f_read.readlines()
        for count, line in enumerate(lines):
            if count == 0:
                continue

            if count < 99999999:
                print(line)
                _splitter = line.split('"')
                _cluster = _splitter[0].split(",")[1]

                _handles = _splitter[1]
                _handles = _handles.replace("[", "")
                _handles = _handles.replace("]", "")
                _handles = _handles.split(",")
                _curated_handles = set()
                for _h in _handles:
                    _h = _h.replace("'", "")
                    _curated_handles.add(_h)
                _clusters[_cluster] = len(_curated_handles)

        _data = []
        for k, v in _clusters.items():
            _data.append((k, v))
        _data.sort(key=lambda x: x[1], reverse=True)

        with open("{}/{}/cluster_data_handle_count.csv".format(_dir_path, _cluster_path), "w") as f_write:
            for val in _data:
                f_write.write("{},{}\n".format(val[0], val[1]))

    def image_cluster_top_30_brands(self):
        _dir_path = "brand_impersonation/report/data_clustering_username_squat"
        _cluster_path = "img_cluster_results"
        f_path = "THR_80/thr_80_results_summary.csv"
        files = {
            "instagram": "{}/{}/instagram/{}".format(_dir_path, _cluster_path, f_path),
            "telegram": "{}/{}/telegram/{}".format(_dir_path, _cluster_path, f_path),
            "twitter": "{}/{}/twitter/{}".format(_dir_path, _cluster_path, f_path),
            "youtube": "{}/{}/youtube/{}".format(_dir_path, _cluster_path, f_path),
        }

        _all_brands = {}

        for social_media, _f in files.items():
            with open(_f, "r") as f_read:
                lines = f_read.readlines()
            for counter, line in enumerate(lines):
                if counter == 0:
                    continue
                _splitter = line.split(",")
                _official = _splitter[0].strip()
                _count = int(_splitter[1])

                if "miscellaneous" in _official:
                    continue

                _official = _official.replace("official_", "")
                if _official not in _all_brands:
                    _all_brands[_official] = _count
                else:
                    _all_brands[_official] = _count + _all_brands[_official]

        _data_ = []
        for k, v in _all_brands.items():
            _data_.append((k, v))

        _data_.sort(key=lambda x: x[1], reverse=True)

        with open("report/graph_data/cluster_top_30_brands_target.txt", "w") as f_write:
            for val in _data_:
                f_write.write("{},{}\n".format(val[0], val[1]))

    def target_30_domain_and_rank_printer(self):
        with open("report/graph_data/top_30_brand_target.json", "r") as f_read:
            data = json.load(f_read)
        domain_name = data['x']

        _data_ = {}
        for d in domain_name:
            _info = []
            for val in db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).find({"name": d}):
                if "domain" in val:
                    _found_domain = val['domain']
                    _info.append((val['domain'], val['rank']))
            _data_[d] = _info
        with open("report/graph_data/top_30_rank_and_domain_name_info.json", "w") as f_write:
            json.dump(_data_, f_write, indent=4)

    def _categorical_official_data(self):
        _all_categorical_official = {}
        all_social_media = ["twitter", "instagram", "youtube", "telegram"]
        _web_category = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN)
                            .distinct(key="web_category", filter={"is_candidate": True}))

        for cat in _web_category:
            _sm_candid = {}
            for sm in all_social_media:
                if sm == "telegram":
                    _candidate = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN)
                                     .distinct(key="name", filter={"is_candidate": True, "web_category": cat}))
                else:
                    _candidate = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN)
                                     .distinct(key=sm,
                                               filter={"is_{}_candidate".format(sm): True, "web_category": cat}))
                lowered = set()
                for c in _candidate:
                    lowered.add(c.lower())
                if sm not in _sm_candid:
                    _sm_candid[sm] = list(lowered)

            _all_categorical_official[cat] = _sm_candid

        return _all_categorical_official

    def web_category_count(self):
        social_media_categorical_official = self._categorical_official_data()
        _all_data = []
        for web_category, social_medias_officials in social_media_categorical_official.items():
            _all_typo = []
            _all_combo_squat = []
            _all_fuzzy = []
            for _each_sm, officials in social_medias_officials.items():
                for _each_official in officials:
                    _typo_squatted_handles = set(
                        shared_util.get_typosquatted_search_handle_from_official_and_social_media(
                            _each_official, _each_sm))
                    _typo_squatted_handles = _typo_squatted_handles.difference(self.officials)

                    for val in _typo_squatted_handles:
                        _all_typo.append(val)

                    _combo_squatted_handles = set(
                        shared_util.get_combo_squatted_search_handle_from_official_and_social_media(
                            _each_official, _each_sm))
                    _combo_squatted_handles = _combo_squatted_handles.difference(self.officials)
                    _combo_squatted_handles = _combo_squatted_handles.difference(_typo_squatted_handles)

                    for val in _combo_squatted_handles:
                        _all_combo_squat.append(val)

                    _fuzzy_squatted_handles = set(
                        shared_util.get_fuzzy_squatted_search_handle_from_official_and_social_media(
                            _each_official, _each_sm
                        ))
                    _fuzzy_squatted_handles = _fuzzy_squatted_handles.difference(self.officials)

                    for val in _fuzzy_squatted_handles:
                        _all_fuzzy.append(val)

            _all_len = len(_all_typo) + len(_all_combo_squat) + len(_all_fuzzy)
            _all_data.append((web_category, len(_all_typo), len(_all_combo_squat), len(_all_fuzzy), _all_len))
        _all_data.sort(key=lambda x: x[4], reverse=True)

        with open("report/attributes/web_category_count/detail.json", "w") as f_write:
            json.dump(_all_data, f_write, indent=4)

    def brands_targeted_count(self):
        all_social_media = ["twitter", "instagram", "youtube", "telegram"]
        _all_data = []
        for official_str in self.officials:
            _all_typo = []
            _all_combo_squat = []
            _all_fuzzy = []
            for social_media in all_social_media:
                _typo_squatted_handles = set(shared_util.get_typosquatted_search_handle_from_official_and_social_media(
                    official_str, social_media))
                _typo_squatted_handles = _typo_squatted_handles.difference(self.officials)

                for val in _typo_squatted_handles:
                    _all_typo.append(val)

                _combo_squatted_handles = set(
                    shared_util.get_combo_squatted_search_handle_from_official_and_social_media(
                        official_str, social_media))
                _combo_squatted_handles = _combo_squatted_handles.difference(self.officials)
                _combo_squatted_handles = _combo_squatted_handles.difference(_typo_squatted_handles)

                for val in _combo_squatted_handles:
                    _all_combo_squat.append(val)

                _fuzzy_squatted_handles = set(
                    shared_util.get_fuzzy_squatted_search_handle_from_official_and_social_media(
                        official_str, social_media
                    ))
                _fuzzy_squatted_handles = _fuzzy_squatted_handles.difference(self.officials)
                for val in _fuzzy_squatted_handles:
                    _all_fuzzy.append(val)

            _all_len = len(_all_typo) + len(_all_combo_squat) + len(_all_fuzzy)
            _all_data.append((official_str, len(_all_typo), len(_all_combo_squat), len(_all_fuzzy), _all_len))
        _all_data.sort(key=lambda x: x[4], reverse=True)

        with open("report/attributes/brands_targeted_count/detail.json", "w") as f_write:
            json.dump(_all_data, f_write, indent=4)

    def overall_social_media_based_target(self):
        _all = {}
        _sm = ["twitter", "instagram", "telegram", "youtube"]
        accounts = set()
        for each in _sm:
            found = set()
            for val in db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find({"social_media": each,
                                                                                        "do_exclude": {
                                                                                            "$exists": False},
                                                                                        "combo_squatting.is_official_word_present": False,
                                                                                        "damerau_levenshtein.distance": {
                                                                                            "$lte": 2}
                                                                                        }):
                found.add(val['search_str'])
            for val in db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find({"social_media": each,
                                                                                        "do_exclude": {
                                                                                            "$exists": False},
                                                                                        "combo_squatting.is_official_word_present": True
                                                                                        }):
                found.add(val['search_str'])

            for val in db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find({"social_media": each,
                                                                                        "do_exclude": {
                                                                                            "$exists": False},
                                                                                        "damerau_levenshtein.distance": 1
                                                                                        }):
                found.add(val['search_str'])

            accounts = accounts.union(found)
            _all[each] = len(found)
        _all['all'] = len(accounts)

        with open("report/attributes/overall_social_media_based_target/overall.json", "w") as f_write:
            json.dump(_all, f_write, indent=4)

    def found_email_brand_target(self):
        _domain_names = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="name", filter={
            "is_candidate": True
        }))

        with open("report/attributes/emails/cleaned_emails.txt", "r") as f_read:
            lines = f_read.readlines()

        brand_target_info = {}
        for _d in _domain_names:
            for line in lines:
                if _d in line:
                    if _d not in brand_target_info:
                        brand_target_info[_d] = 1
                    else:
                        brand_target_info[_d] = 1 + brand_target_info[_d]

        _data = []
        for k, v in brand_target_info.items():
            _data.append((k, v))
        _data.sort(key=lambda x: x[1], reverse=True)

        with open("report/attributes/emails/brand_targeted_report.txt", "w") as f_read:
            json.dump(_data, f_read, indent=4)

    def get_lowered_case_from_list(self, lst):
        create = set()
        for val in lst:
            create.add(val.lower())
        return create

    def get_all_search_accounts(self):
        _all_search_accounts = set()
        _sm = ["twitter", "instagram", "telegram", "youtube"]
        for each in _sm:
            for val in db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find({"social_media": each,
                                                                                        "do_exclude": {"$exists": False}
                                                                                        }):
                _all_search_accounts.add(val['search_str'])
        return _all_search_accounts

    def post_metrics(self):
        _inspect_posts = []
        _all_search_accounts = self.get_all_search_accounts()
        _twitter_posts = {}
        _max = 100000
        for val in db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).find():
            if 'data' not in val:
                continue
            _data = val['data'][0]
            public_metrics = _data['public_metrics']
            posts = public_metrics['tweet_count']
            username = _data['username']
            username = username.lower()
            if username not in _all_search_accounts:
                continue
            if username not in _twitter_posts:
                if posts > _max:
                    _inspect_posts.append((_data['username'], posts, 'twitter'))
                    continue
                _twitter_posts[username] = posts

        _insta_posts = {}
        for val in db_util.MongoDBActor(constants.COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).find():
            if 'username' not in val:
                continue
            username = val['username']
            username = username.lower()
            if username not in _all_search_accounts:
                continue
            if 'postsCount' not in val:
                continue
            _posts_count = val['postsCount']
            if username not in _insta_posts:
                if _posts_count > _max:
                    _inspect_posts.append((username, _posts_count, 'instagram'))
                    continue
                _insta_posts[username] = _posts_count

        _telegram_posts = {}
        for val in db_util.MongoDBActor(constants.COLLECTIONS.TELEGRAM_APIFY_SEARCH).find():
            if 'channelName' not in val:
                continue
            username = val['channelName']
            username = username.lower()
            if username not in _all_search_accounts:
                continue
            if 'viewsCount' not in val:
                continue
            _posts_count = val['viewsCount']
            if not _posts_count:
                continue
            if type(_posts_count) is str and "K" in _posts_count:
                _posts_count = _posts_count.replace("K", "")
                if "." in _posts_count:
                    _posts_count = _posts_count.split(".")[0]
                _posts_count = int(_posts_count) * 1000

            if type(_posts_count) is str and "M" in _posts_count:
                _posts_count = _posts_count.replace("M", "")
                if "." in _posts_count:
                    _posts_count = _posts_count.split(".")[0]
                _posts_count = int(_posts_count) * 1000000
            if type(_posts_count) is not int:
                _posts_count = int(_posts_count)
            if username not in _telegram_posts:
                if _posts_count > _max:
                    _inspect_posts.append((username, _posts_count, 'telegram'))
                _telegram_posts[username] = _posts_count

        _youtube_posts = {}
        for val in db_util.MongoDBActor(constants.COLLECTIONS.YOUTUBE_APIFY_SEARCH).find():
            if 'channelUrl' not in val:
                continue
            username = val['channelUrl']
            if not username:
                continue
            username = shared_util.get_youtube_handle_from_channel_url(username)
            username = username.lower()
            if username not in _all_search_accounts:
                continue
            if 'commentsCount' not in val:
                continue
            commentsCount = val['commentsCount']
            if username not in _youtube_posts:
                if commentsCount > _max:
                    _inspect_posts.append((val['channelUrl'], commentsCount, 'youtube'))
                    continue
                _youtube_posts[username] = commentsCount

        public_data = {
            'Twitter': list(_twitter_posts.values()),
            'Instagram': list(_insta_posts.values()),
            'YouTube': list(_youtube_posts.values()),
            'Telegram': list(_telegram_posts.values())
        }

        _inspect_posts.sort(key=lambda x: x[1], reverse=True)
        with open("report/attributes/public_metrics/posts_manual_analysis.csv", "w") as f_write:
            for val in _inspect_posts:
                f_write.write("{},{},{}\n".format(val[0], val[1], val[2]))

        with open("report/attributes/public_metrics/posts.json", "w") as f_write:
            json.dump(public_data, f_write, indent=4)

    def followers_metrics(self):
        _inspect_followers = []
        _all_search_accounts = self.get_all_search_accounts()
        _twitter_followers = {}
        _max = 300000
        for val in db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).find():
            if 'data' not in val:
                continue
            _data = val['data'][0]
            public_metrics = _data['public_metrics']
            followers = public_metrics['followers_count']
            username = _data['username']
            username = username.lower()
            if username not in _all_search_accounts:
                continue
            if username not in _twitter_followers:
                if followers > _max:
                    _inspect_followers.append((_data['username'], followers, 'twitter'))
                    continue
                _twitter_followers[username] = followers

        _insta_followers = {}
        for val in db_util.MongoDBActor(constants.COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).find():
            if 'username' not in val:
                continue
            username = val['username']
            username = username.lower()
            if username not in _all_search_accounts:
                continue
            if 'followersCount' not in val:
                continue
            _followers_count = val['followersCount']
            if username not in _insta_followers:
                if _followers_count > _max:
                    _inspect_followers.append((username, _followers_count, 'instagram'))
                    continue
                _insta_followers[username] = _followers_count

        _telegram_followers = {}
        for val in db_util.MongoDBActor(constants.COLLECTIONS.TELEGRAM_CHANNELS).find():
            if 'channel_handle' not in val:
                continue
            username = val['channel_handle']
            username = username.lower()
            if username not in _all_search_accounts:
                continue
            if 'subscriber' not in val:
                continue
            _followers_count = val['subscriber']
            if not _followers_count:
                continue
            _followers_count = _followers_count.replace(" ", "")
            _followers_count = int(_followers_count)
            if username not in _telegram_followers:
                if _followers_count > _max:
                    _inspect_followers.append((username, _followers_count, 'telegram'))
                    continue
                _telegram_followers[username] = _followers_count

        _youtube_followers = {}
        for val in db_util.MongoDBActor(constants.COLLECTIONS.YOUTUBE_APIFY_SEARCH).find():
            if 'channelUrl' not in val:
                continue
            username = val['channelUrl']
            if not username:
                continue
            username = shared_util.get_youtube_handle_from_channel_url(username)
            username = username.lower()
            if username not in _all_search_accounts:
                continue
            if 'numberOfSubscribers' not in val:
                continue
            _followers_count = val['numberOfSubscribers']
            if username not in _telegram_followers:
                if _followers_count > _max:
                    _inspect_followers.append((val['channelUrl'], _followers_count, 'youtube'))
                    continue
                _youtube_followers[username] = _followers_count

        public_data = {
            'Twitter': list(_twitter_followers.values()),
            'Instagram': list(_insta_followers.values()),
            'YouTube': list(_youtube_followers.values()),
            'Telegram': list(_telegram_followers.values())
        }

        _inspect_followers.sort(key=lambda x: x[1], reverse=True)
        with open("report/attributes/public_metrics/followers_manual_analysis.csv", "w") as f_write:
            for val in _inspect_followers:
                f_write.write("{},{},{}\n".format(val[0], val[1], val[2]))

        with open("report/attributes/public_metrics/followers.json", "w") as f_write:
            json.dump(public_data, f_write, indent=4)

    def fetch_twitter_account_details_for_remaining_search_accounts_users(self):
        _all_typo_squatted_accounts = set(shared_util.get_typosquatted_search_handle())
        _all_typo_squatted_accounts = set(shared_util.get_combo_squatted_search_handle())
        _all_fuzzy_squatted_accounts = set(shared_util.get_fuzzy_squatted_search_handle())

        _all_search_accounts = _all_typo_squatted_accounts.union(_all_typo_squatted_accounts).union(
            _all_fuzzy_squatted_accounts)

        twitter_suspended_accounts_user_detail = set(
            db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).distinct(key="username", filter={
                "errors.title": 'Forbidden'}))
        twitter_suspended_accounts_timelines = set(
            db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_TIMELINESS).distinct(key="username", filter={
                "errors.title": 'Forbidden'}))
        twitter_not_alive = self.get_lowered_case_from_list(twitter_suspended_accounts_user_detail.union(
            twitter_suspended_accounts_timelines))

        _all_accounts_found_in_our_db = set()

        for val in db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).find():
            if 'username' in val:
                found = val['username']
                found = found.lower()
                _all_accounts_found_in_our_db.add(found)

        _all_accounts_found_in_our_db = _all_accounts_found_in_our_db.union(twitter_not_alive)

        _fetch_accounts_detail = _all_search_accounts.difference(_all_accounts_found_in_our_db)
        _len_to_fetch = len(_fetch_accounts_detail)
        print("Total to process :{}".format(len(_fetch_accounts_detail)))
        for count, account in enumerate(_fetch_accounts_detail):
            print("{}/{}, {}".format(count, _len_to_fetch, account))
            shared_util.fetch_user_if_not_present(account)
            if count == 25000:  # twitter's limit
                break

    def blocked_account(self):
        _all_data = {}

        _domain_names = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="name", filter={
            "is_candidate": True
        }))

        _twitter_typo_squatted_accounts = set(shared_util.get_typosquatted_search_handle_of_social_media('twitter'))
        _twitter_combo_squatted_accounts = set(
            shared_util.get_combo_squatted_search_handle_from_social_media('twitter'))
        _twitter_fuzzy_squatted_accounts = set(
            shared_util.get_fuzzy_squatted_search_handle_from_social_media('twitter'))
        _all_twitter_accounts = _twitter_typo_squatted_accounts.union(_twitter_combo_squatted_accounts).union(
            _twitter_fuzzy_squatted_accounts)

        _instagram_typo_squatted_accounts = set(shared_util.get_typosquatted_search_handle_of_social_media('instagram'))
        _instagram_combo_squatted_accounts = set(
            shared_util.get_combo_squatted_search_handle_from_social_media('instagram'))
        _instagram_fuzzy_squatted_accounts = set(
            shared_util.get_fuzzy_squatted_search_handle_from_social_media('instagram'))
        _all_instagram_accounts = _instagram_typo_squatted_accounts.union(_instagram_combo_squatted_accounts).union(
            _instagram_fuzzy_squatted_accounts)

        _telegram_typo_squatted_accounts = set(shared_util.get_typosquatted_search_handle_of_social_media('telegram'))
        _telegram_combo_squatted_accounts = set(
            shared_util.get_combo_squatted_search_handle_from_social_media('telegram'))
        _telegram_fuzzy_squatted_accounts = set(
            shared_util.get_fuzzy_squatted_search_handle_from_social_media('telegram'))
        _all_telegram_accounts = _telegram_typo_squatted_accounts.union(_telegram_combo_squatted_accounts).union(
            _telegram_fuzzy_squatted_accounts)

        _youtube_typo_squatted_accounts = set(shared_util.get_typosquatted_search_handle_of_social_media('youtube'))
        _youtube_combo_squatted_accounts = set(
            shared_util.get_combo_squatted_search_handle_from_social_media('youtube'))
        _youtube_fuzzy_squatted_accounts = set(
            shared_util.get_fuzzy_squatted_search_handle_from_social_media('youtube'))
        _all_youtube_accounts = _youtube_typo_squatted_accounts.union(_youtube_combo_squatted_accounts).union(
            _youtube_fuzzy_squatted_accounts)

        _all_search_accounts = self.get_all_search_accounts()
        twitter_suspended_accounts_user_detail = set(
            db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).distinct(key="username", filter={
                "errors.title": 'Forbidden'}))
        twitter_suspended_accounts_timelines = set(
            db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_TIMELINESS).distinct(key="username", filter={
                "errors.title": 'Forbidden'}))
        twitter_suspended_accounts = twitter_suspended_accounts_user_detail.union(twitter_suspended_accounts_timelines)
        twitter_suspended_accounts = self.get_lowered_case_from_list(twitter_suspended_accounts)

        twitter_deleted_accounts_user_detail = set(
            db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).distinct(key="username", filter={
                "errors.title": 'Not Found Error'}))
        twitter_deleted_accounts_timelines = set(
            db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_TIMELINESS).distinct(key="username", filter={
                "errors.title": 'Not Found Error'}))
        twitter_deleted_accounts = twitter_deleted_accounts_user_detail.union(twitter_deleted_accounts_timelines)
        twitter_deleted_accounts = self.get_lowered_case_from_list(twitter_deleted_accounts)
        twitter_not_alive = twitter_suspended_accounts.union(twitter_deleted_accounts)
        twitter_not_alive = self.get_lowered_case_from_list(twitter_not_alive)

        telegram_blocked_accounts = set(
            db_util.MongoDBActor(constants.COLLECTIONS.TELEGRAM_CHANNELS).distinct(key="channel_handle", filter={
                "is_blocked": True}))
        telegram_blocked_accounts = self.get_lowered_case_from_list(telegram_blocked_accounts)
        telegram_blocked_accounts = telegram_blocked_accounts.intersection(_all_search_accounts)

        _found_instagram_blocked_accounts = set(db_util.MongoDBActor(constants.
                                                                     COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA)
                                                .distinct(key="inputUrl", filter={"error": {"$exists": True}}))
        instagram_blocked_accounts = set()
        for val in _found_instagram_blocked_accounts:
            line = val.replace("https://www.instagram.com/", "")
            instagram_blocked_accounts.add(line)

        _found_yoututbe_blocked_accounts = set(
            db_util.MongoDBActor(constants.COLLECTIONS.YOUTUBE_APIFY_SEARCH).distinct(key="channelUrl", filter={
                "is_alive": False
            }))

        youtube_blocked_accounts = set()
        for val in _found_yoututbe_blocked_accounts:
            _handle = shared_util.get_youtube_handle_from_channel_url(val)
            youtube_blocked_accounts.add(_handle.lower())

        _all_data['twitter_all_accounts_squatting'] = len(_all_twitter_accounts)
        _all_data['twitter_suspended'] = len(twitter_suspended_accounts)
        _all_data['twitter_deleted'] = len(twitter_deleted_accounts)
        _all_data['twitter_not_alive'] = len(twitter_not_alive)
        _all_data['twitter_typosquatted_blocked'] = len(_twitter_typo_squatted_accounts.intersection(twitter_not_alive))
        _all_data['twitter_combosquatted_blocked'] = len(
            _twitter_combo_squatted_accounts.intersection(twitter_not_alive))
        _all_data['twitter_fuzzysquatted_blocked'] = len(
            _twitter_fuzzy_squatted_accounts.intersection(twitter_not_alive))

        _all_data['telegram_all_accounts_squatting'] = len(_all_telegram_accounts)
        _all_data['telegram_typosquatted_blocked'] = len(
            _telegram_typo_squatted_accounts.intersection(telegram_blocked_accounts))
        _all_data['telegram_combosquatted_blocked'] = len(
            _telegram_combo_squatted_accounts.intersection(telegram_blocked_accounts))
        _all_data['telegram_fuzzysquatted_blocked'] = len(
            _telegram_fuzzy_squatted_accounts.intersection(telegram_blocked_accounts))
        _all_data['telegram_blocked'] = len(telegram_blocked_accounts)
        _all_data['telegram_blocked_accounts'] = list(telegram_blocked_accounts)

        _all_data['instagram_all_accounts_squatting'] = len(_all_instagram_accounts)
        _all_data['instagram_typosquatted_blocked'] = len(
            _instagram_typo_squatted_accounts.intersection(instagram_blocked_accounts))
        _all_data['instagram_combosquatted_blocked'] = len(
            _instagram_combo_squatted_accounts.intersection(instagram_blocked_accounts))
        _all_data['instagram_fuzzysquatted_blocked'] = len(
            _instagram_fuzzy_squatted_accounts.intersection(instagram_blocked_accounts))
        _all_data['instagram_blocked'] = len(instagram_blocked_accounts)
        _all_data['instagram_blocked_accounts'] = list(instagram_blocked_accounts)

        _all_data['youtube_all_accounts_squatting'] = len(_all_youtube_accounts)
        _all_data['youtube_typosquatted_blocked'] = len(
            _youtube_typo_squatted_accounts.intersection(youtube_blocked_accounts))
        _all_data['youtube_combosquatted_blocked'] = len(
            _youtube_combo_squatted_accounts.intersection(youtube_blocked_accounts))
        _all_data['youtube_fuzzysquatted_blocked'] = len(
            _youtube_fuzzy_squatted_accounts.intersection(youtube_blocked_accounts))
        _all_data['youtube_blocked'] = len(youtube_blocked_accounts)
        _all_data['youtube_blocked_accounts'] = list(youtube_blocked_accounts)

        twitter_blocked_brand_info = self.get_attacking_brand_from_found_blocked_data(_domain_names, twitter_not_alive)
        telegram_blocked_brand_info = self.get_attacking_brand_from_found_blocked_data(_domain_names,
                                                                                       telegram_blocked_accounts)
        instagram_blocked_brand_info = self.get_attacking_brand_from_found_blocked_data(_domain_names,
                                                                                        instagram_blocked_accounts)
        youtube_blocked_brand_info = self.get_attacking_brand_from_found_blocked_data(_domain_names,
                                                                                        youtube_blocked_accounts)
        _all_data['twitter_brand_blocked_info'] = self.get_sorted_tuple_from_dictionary(twitter_blocked_brand_info)
        _all_data['telegram_brand_blocked_info'] = self.get_sorted_tuple_from_dictionary(telegram_blocked_brand_info)
        _all_data['instagram_brand_blocked_info'] = self.get_sorted_tuple_from_dictionary(instagram_blocked_brand_info)
        _all_data['youtube_brand_blocked_info'] = self.get_sorted_tuple_from_dictionary(youtube_blocked_brand_info)

        with open("report/attributes/blocked_accounts/detail.json", "w") as f_write:
            json.dump(_all_data, f_write, indent=4)

    def get_attacking_brand_from_found_blocked_data(self, domain_names, blocked_account):
        blocked_brand_info = {}
        for name in domain_names:
            for twt_acc in blocked_account:
                if name in twt_acc:
                    if name not in blocked_brand_info:
                        blocked_brand_info[name] = 1
                    else:
                        blocked_brand_info[name] = 1 + blocked_brand_info[name]
        return blocked_brand_info

    def get_sorted_tuple_from_dictionary(self, _dict):
        _data = []
        for k, v in _dict.items():
            _data.append((k, v))
        _data.sort(key=lambda x: x[1], reverse=True)
        return _data

    def sorting(self, l):
        splitup = l.split('-')
        return splitup[0], splitup[1]

    def account_creation_date(self):
        _social_media = ["twitter", "instagram", "telegram", "youtube"]
        _all_officials = set()
        for _sm in _social_media:
            _search_str = set(
                db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).distinct(key="official_str", filter={
                    "social_media": _sm
                }))
            _all_officials = _all_officials.union(_search_str)

        _telegram = {}
        for val in db_util.MongoDBActor(constants.COLLECTIONS.TELEGRAM_APIFY_SEARCH).find():
            if 'date' in val:
                _found_data = val['date']
                if not _found_data:
                    continue
                _found_data = _found_data.split("-")[0]
                _found_data_year = int(_found_data)
                channel_name = val['channelName']

                if channel_name.lower() not in _all_officials:
                    if _found_data_year not in _telegram:
                        _telegram[_found_data_year] = [channel_name]
                    else:
                        _telegram[_found_data_year] = list(set(_telegram[_found_data_year] + [channel_name]))

        _twitter = {}
        for val in db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).find():
            if 'data' not in val:
                continue
            data = val['data'][0]
            _found_data = data['created_at']
            print(_found_data)
            _found_data_year = int(_found_data.split("-")[0])
            channel_name = data['username']

            if channel_name.lower() not in _all_officials:
                if _found_data_year not in _twitter:
                    _twitter[_found_data_year] = [channel_name]
                else:
                    _twitter[_found_data_year] = list(set(_twitter[_found_data_year] + [channel_name]))

        _youtube = {}
        for val in db_util.MongoDBActor(constants.COLLECTIONS.YOUTUBE_APIFY_SEARCH).find():
            if 'date' not in val:
                continue
            _found_data = val['date']
            print(_found_data)
            if not _found_data:
                continue
            if "-" not in _found_data:
                continue
            _found_data_year = int(_found_data.split("-")[0])
            if 'channelName' not in val:
                continue
            channel_name = val['channelName']
            print(channel_name)
            if not channel_name:
                continue
            if channel_name.lower() not in _all_officials:
                if _found_data_year not in _youtube:
                    _youtube[_found_data_year] = [channel_name]
                else:
                    _youtube[_found_data_year] = list(set(_youtube[_found_data_year] + [channel_name]))

        _instagram = {}
        for val in db_util.MongoDBActor(constants.COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).find():
            if "latestPosts" not in val:
                continue

            _found_latest_posts = val['latestPosts']
            _all_time_stamp = set()
            for _each_post in _found_latest_posts:
                _found_date = _each_post['timestamp']
                _all_time_stamp.add(_found_date)

            if len(_all_time_stamp) < 1:
                continue

            _all_time_stamp = list(_all_time_stamp)
            _all_time_stamp.sort(key=self.sorting)

            _found_data = _all_time_stamp[0]
            print(_found_data)
            if not _found_data:
                continue
            if "-" not in _found_data:
                continue
            _found_data_year = int(_found_data.split("-")[0])
            if 'username' not in val:
                continue
            channel_name = val['username']
            print(channel_name)
            if not channel_name:
                continue
            if channel_name.lower() not in _all_officials:
                if _found_data_year not in _instagram:
                    _instagram[_found_data_year] = [channel_name]
                else:
                    _instagram[_found_data_year] = list(set(_instagram[_found_data_year] + [channel_name]))

        _social_media = {
            'twitter': _twitter,
            'youtube': _youtube,
            'telegram': _telegram,
            'instagram': _instagram
        }

        with open("report/attributes/account_creation/data.json", "w") as f_write:
            json.dump(_social_media, f_write, indent=4)

    def profile_description_urls(self):
        each_contribitions = {}
        _distinct_twitter_urls = set(db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).distinct(key="data.url"))
        each_contribitions["twitter_count"] = len(_distinct_twitter_urls)
        _distinct_instagram_urls = set(
            db_util.MongoDBActor(constants.COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).distinct(key="externalUrl"))
        each_contribitions["instagram"] = len(_distinct_instagram_urls)
        _all_urls = _distinct_twitter_urls.union(_distinct_instagram_urls)
        youtube_curated_text_url = set()
        telegram_curated_text_url = set()

        for val in db_util.MongoDBActor(constants.COLLECTIONS.YOUTUBE_APIFY_SEARCH).find(
                {"text": {"$regex": "https://"}}):
            if 'text' not in val:
                continue
            youtube_curated_text_url.add(val['text'])
        for val in db_util.MongoDBActor(constants.COLLECTIONS.TELEGRAM_CHANNELS).find(
                {"info_content": {"$regex": "https://"}}):
            if 'info_content' not in val:
                continue
            telegram_curated_text_url.add(val['info_content'])

        _found_youtube = set()
        for cnt, _c in enumerate(youtube_curated_text_url):
            _found_url = shared_util.get_url_from_line(_c)
            print("Processing {}, {}, {}".format(cnt, _c, _found_url))
            if _found_url:
                _found_youtube.add(_found_url)
                _all_urls.add(_found_url)

        found_telegram = set()
        for cnt, _c in enumerate(telegram_curated_text_url):
            _found_url = shared_util.get_url_from_line(_c)
            print("Processing {}, {}, {}".format(cnt, _c, _found_url))
            if _found_url:
                found_telegram.add(_found_url)
                _all_urls.add(_found_url)
        each_contribitions['telegram'] = len(found_telegram)
        each_contribitions['youtube'] = len(_found_youtube)
        with open("report/attributes/profile_description_urls/urls.txt", "w") as f_read:
            for val in _all_urls:
                is_valid_url = validators.url(val)
                if is_valid_url:
                    f_read.write("{}\n".format(val))

        _intersected = set()
        _urls_attacking = set()
        _all_domains = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="domain"))
        _contained_domain = set()
        for _all_d in _all_domains:
            for al in _all_urls:
                if _all_d in al:
                    _intersected.add(_all_d)
                    _urls_attacking.add(al)

        json_put = {
            'attack_brand': list(_intersected),
            'each_attack_contribution': each_contribitions
        }
        with open("report/attributes/profile_description_urls/total_top_10K_intersect.txt", "w") as f_read:
            json.dump(json_put, f_read, indent=4)

    def get_links(self):
        _distinct_twitter_links = set(db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).distinct(key="data.url"))

    def all_official(self):
        _distinct_twitter_official = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="twitter",
                                                                                                     filter={
                                                                                                         "is_candidate": True}))
        _distinct_youtube_official = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="youtube",
                                                                                                     filter={
                                                                                                         "is_candidate": True}))
        _distinct_instagram_official = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="instagram",
                                                                                                       filter={
                                                                                                           "is_candidate": True}))

        _distinct_telegram_official = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="name",
                                                                                                      filter={
                                                                                                          "is_candidate": True}))

        return list(
            _distinct_twitter_official.union(_distinct_youtube_official).union(_distinct_instagram_official).union(
                _distinct_telegram_official)
        )

    def location_info(self):
        _location_user = {}
        _distinct_twitter_location = set(
            db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).distinct(key="data.location", filter={"username":
                                                                                                               {"$nin":
                                                                                                                    self.officials}}))
        for dl in _distinct_twitter_location:
            _users = set(
                db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).distinct(key="username",
                                                                                  filter={"data.location": dl,
                                                                                          "username":
                                                                                              {"$nin":
                                                                                                   self.officials}}))
            if dl not in _location_user:
                _location_user[dl] = list(_users)
            else:
                _prev = _location_user[dl] + list(_users)
                _prev = set(_prev)
                _location_user[dl] = list(_prev)

        _distinct_youtube_location = set(
            db_util.MongoDBActor(constants.COLLECTIONS.YOUTUBE_APIFY_SEARCH).distinct(key="location",
                                                                                      filter={"channelName":
                                                                                                  {"$nin":
                                                                                                       self.officials}}))
        for dl in _distinct_youtube_location:
            _users = set(
                db_util.MongoDBActor(constants.COLLECTIONS.YOUTUBE_APIFY_SEARCH).distinct(key="channelUrl",
                                                                                          filter={"location": dl,
                                                                                                  "channelName":
                                                                                                      {"$nin":
                                                                                                           self.officials}}))
            if dl not in _location_user:
                _location_user[dl] = list(_users)
            else:
                _prev = _location_user[dl] + list(_users)
                _prev = set(_prev)
                _location_user[dl] = list(_prev)

        _all_data = []
        for k, v in _location_user.items():
            _all_data.append((k, len(v)))

        _all_data.sort(key=lambda x: x[1], reverse=True)

        with open("report/attributes/profile_bio_info/location.csv", "w") as f_write:
            for val in _all_data:
                f_write.write("{}, {}\n".format(val[0], val[1]))

    def human_name_info(self):
        _human_name_info = {}
        _distinct_twitter_name = set(
            db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).distinct(key="data.name", filter={"username":
                                                                                                           {"$nin":
                                                                                                                self.officials}}))

        _distinct_youtube_name = set(
            db_util.MongoDBActor(constants.COLLECTIONS.YOUTUBE_APIFY_SEARCH).distinct(key="channelName",
                                                                                      filter={"channelName":
                                                                                                  {"$nin":
                                                                                                       self.officials}}))

        _distinct_instagram_name = set(
            db_util.MongoDBActor(constants.COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).distinct(key="fullName",
                                                                                               filter={"username":
                                                                                                           {"$nin":
                                                                                                                self.officials}}))

        _distinct_telegram_name = set(
            db_util.MongoDBActor(constants.COLLECTIONS.TELEGRAM_APIFY_SEARCH).distinct(key="authorName",
                                                                                       filter={"channelName":
                                                                                                   {"$nin":
                                                                                                        self.officials}}))

        _all_names = _distinct_youtube_name.union(_distinct_instagram_name).union(_distinct_twitter_name)
        _official_intersect = list(_all_names.intersection(self.officials))

        with open("brand_impersonation_data/top10milliondomains.csv", "r") as f_read:
            lines = f_read.readlines()

        two_tld = set()
        for cnt, line in enumerate(lines):
            if cnt == 0:
                continue
            if cnt == 1000000:
                break

            split = line.split(",")
            domain = split[1]
            domain = domain.replace('"', "")
            domain = domain.split(".")[0]
            domain = domain.strip()
            two_tld.add(domain)

        _lower_case_name = set()

        for name in _all_names:
            _lower_case_name.add(name.lower())

        _found_human_name = []
        for _n in _lower_case_name:
            sent = nlp(_n)
            for token in sent:
                if token.ent_type_ == 'PERSON':
                    _found_human_name.append(token)

        _human_name_info = {
            '2ld_match': len(two_tld),
            'official_intersect_match': len(_official_intersect),
            'human_name_match': len(_found_human_name),
            '2ld_matched': list(two_tld),
            'human_names': _found_human_name,
            'official_intersect': list(_official_intersect),
            'all_names': list(_all_names)
        }

        with open("report/attributes/profile_bio_info/human_name_info.json", "w") as f_write:
            json.dump(_human_name_info, f_write, indent=4)


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Create profile metadata info")
    _arg_parser.add_argument("-f", "--function",
                             action="store",
                             required=True,
                             help="processing social media ")

    _arg_value = _arg_parser.parse_args()

    pm = ProfileMetaData(_arg_value.function)
    pm.process()
