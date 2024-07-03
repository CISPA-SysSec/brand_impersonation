import argparse
import random
import constants
import shared_util
import db_util
import os

POST_DIR_PATH = "brand_impersonation_data/posts"


class ClusterData:
    def __init__(self, social_media):
        self.social_media = social_media

    def process(self):
        if self.social_media == "twitter":
            self.process_twitter_user_files()
        elif self.social_media == "instagram":
            self.process_instagram_user_files()
        elif self.social_media == "telegram":
            self.process_telegram_user_files()
        elif self.social_media == "youtube":
            self.process_youtube_user_files()

    def text_similarity_users(self):
        text_similarity_users = set()
        for val in db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find({
            "do_exclude": {"$exists": False},
            "social_media": self.social_media
        }):
            if 'search_str' in val:
                text_similarity_users.add(val['search_str'])
        return text_similarity_users

    def process_youtube_user_files(self):
        text_similarity_users = self.text_similarity_users()
        _total_len = len(text_similarity_users)

        print("Total text similarity users:{}".format(_total_len))
        _found_time_line_users = set(
            db_util.MongoDBActor(constants.COLLECTIONS.YOUTUBE_APIFY_SEARCH).distinct(key="channelUrl"))
        _intersection_users = set()
        for val in _found_time_line_users:
            _channel = shared_util.get_youtube_handle_from_channel_url(val)
            if _channel.lower() in text_similarity_users:
                _intersection_users.add(val)

        _intersected_len = len(_intersection_users)
        print("Intersected users:{}".format(len(_intersection_users)))

        for cnt_user, username in enumerate(_intersection_users):
            print("Processing {}/{}, username:{}".format(cnt_user, _intersected_len, username))
            _fname = "{}/{}/{}.csv".format(POST_DIR_PATH, self.social_media,
                                           shared_util.get_youtube_handle_from_channel_url(username))
            if os.path.exists(_fname):
                print('Already processed .. Escaping this file {}'.format(_fname))
                continue
            _distinct_posts = set(
                db_util.MongoDBActor(constants.COLLECTIONS.YOUTUBE_APIFY_SEARCH).distinct(
                    key="text",
                    filter={
                        "channelUrl": username}))
            if None in _distinct_posts:
                _distinct_posts.remove(None)

            print("Posts length found:{}, {}".format(len(_distinct_posts), username))
            if len(_distinct_posts) > 0:
                print("Posts file created:{}, f_name:{}, social_media:{}".format(username, _fname, self.social_media))
                with open(_fname, "w") as f_write:
                    for cnt, line in enumerate(_distinct_posts):
                        line = line.replace(",", "")
                        line = line.replace("\n", "")
                        if len(line) < 10:
                            continue
                        f_write.write("----start-----\n{}\n----end-----\n".format(line))

    def process_telegram_user_files(self):
        text_similarity_users = self.text_similarity_users()
        _total_len = len(text_similarity_users)

        print("Total text similarity users:{}".format(_total_len))
        _found_time_line_users = set(
            db_util.MongoDBActor(constants.COLLECTIONS.TELEGRAM_APIFY_SEARCH).distinct(key="channelName"))
        _intersection_users = set()
        for val in _found_time_line_users:
            if val.lower() in text_similarity_users:
                _intersection_users.add(val)

        _intersected_len = len(_intersection_users)
        print("Intersected users:{}".format(len(_intersection_users)))

        for cnt_user, username in enumerate(_intersection_users):
            print("Processing {}/{}, username:{}".format(cnt_user, _intersected_len, username))
            _fname = "{}/{}/{}.csv".format(POST_DIR_PATH, self.social_media, username)
            if os.path.exists(_fname):
                print('Already processed .. Escaping this file {}'.format(_fname))
                continue
            _distinct_posts = set(
                db_util.MongoDBActor(constants.COLLECTIONS.TELEGRAM_APIFY_SEARCH).distinct(
                    key="text",
                    filter={
                        "channelName": username}))
            if None in _distinct_posts:
                _distinct_posts.remove(None)

            print("Posts length found:{}, {}".format(len(_distinct_posts), username))
            if len(_distinct_posts) > 0:
                print("Posts file created:{}, f_name:{}, social_media:{}".format(username, _fname, self.social_media))
                with open(_fname, "w") as f_write:
                    for cnt, line in enumerate(_distinct_posts):
                        line = line.replace(",", "")
                        line = line.replace("\n", "")
                        if len(line) < 10:
                            continue
                        f_write.write("----start-----\n{}\n----end-----\n".format(line))

    def process_instagram_user_files(self):
        text_similarity_users = self.text_similarity_users()
        _total_len = len(text_similarity_users)

        print("Total text similarity users:{}".format(_total_len))
        _found_time_line_users = set(
            db_util.MongoDBActor(constants.COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).distinct(key="username"))
        _intersection_users = set()
        for val in _found_time_line_users:
            if val.lower() in text_similarity_users:
                _intersection_users.add(val)

        _intersected_len = len(_intersection_users)
        print("Intersected users:{}".format(len(_intersection_users)))

        for cnt_user, username in enumerate(_intersection_users):
            print("Processing {}/{}, username:{}".format(cnt_user, _intersected_len, username))
            _fname = "{}/{}/{}.csv".format(POST_DIR_PATH, self.social_media, username)
            if os.path.exists(_fname):
                print('Already processed .. Escaping this file {}'.format(_fname))
                continue
            _distinct_videos_posts_caption = set(
                db_util.MongoDBActor(constants.COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).distinct(
                    key="latestIgtvVideos.caption",
                    filter={
                        "username": username}))
            _distinct_latest_posts_caption = set(
                db_util.MongoDBActor(constants.COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).distinct(
                    key="latestPosts.caption",
                    filter={
                        "username":
                            username
                    }))
            _distinct_child_posts_caption = set(
                db_util.MongoDBActor(constants.COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).distinct(
                    key="childPosts.caption",
                    filter={
                        "username":
                            username
                    }))
            _distinct_posts = _distinct_videos_posts_caption.union(_distinct_latest_posts_caption).union(
                _distinct_child_posts_caption)
            if None in _distinct_posts:
                _distinct_posts.remove(None)

            print("Posts length found:{}, {}".format(len(_distinct_posts), username))
            if len(_distinct_posts) > 0:
                print("Posts file created:{}, f_name:{}".format(username, _fname))
                with open(_fname, "w") as f_write:
                    for cnt, line in enumerate(_distinct_posts):
                        line = line.replace(",", "")
                        line = line.replace("\n", "")
                        if len(line) < 10:
                            continue
                        f_write.write("----start-----\n{}\n----end-----\n".format(line))

    def process_twitter_user_files(self):
        text_similarity_users = self.text_similarity_users()
        _total_len = len(text_similarity_users)

        print("Total text similarity users:{}".format(_total_len))
        _time_line_users = set()
        _found_times_users = db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_TIMELINESS).distinct(key="username")
        if None in _found_times_users:
            _time_line_users.add(None)
        _intersection_users = set()
        for val in _found_times_users:
            if val.lower() in text_similarity_users:
                _intersection_users.add(val)
        _intersected_len = len(_intersection_users)
        print("Intersected users:{}".format(len(_intersection_users)))

        for cnt_user, username in enumerate(_intersection_users):
            print("Processing {}/{}, username:{}".format(cnt_user, _intersected_len, username))
            _fname = "{}/{}/{}.csv".format(POST_DIR_PATH, self.social_media, username)
            if os.path.exists(_fname):
                print('Already processed .. Escaping this file {}'.format(_fname))
                continue
            _distinct_posts = set(db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_TIMELINESS).distinct(key="text",
                                                                                                          filter={
                                                                                                              "username": username}))

            if None in _distinct_posts:
                _distinct_posts.remove(None)
            print("Posts length found:{}, {}".format(len(_distinct_posts), username))
            if len(_distinct_posts) > 0:
                print("Posts file created:{}, f_name:{}".format(username, _fname))
                with open(_fname, "w") as f_write:
                    for cnt, line in enumerate(_distinct_posts):
                        line = line.replace(",", "")
                        line = line.replace("\n", "")
                        if len(line) < 10:
                            continue
                        f_write.write("----start-----\n{}\n----end-----\n".format(line))


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Create cluster data")
    _arg_parser.add_argument("-f", "--function_name",
                             action="store",
                             required=True,
                             help="processing function name")

    _arg_value = _arg_parser.parse_args()

    _cluster_ = ClusterData(_arg_value.function_name)
    _cluster_.process()
