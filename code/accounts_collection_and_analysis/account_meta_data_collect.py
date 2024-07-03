import json

import db_util
import constants
import os
import time
import twitter_api
import shared_util
import argparse
from selenium_driver import PageDriver
import hashlib
import re

IMG_DIR_PATH = "images/"
EMAIL_REGEX = re.compile(r"[^@]+@[^@]+\.[^@]+")


class TwitterAccountMetaDataCollector:
    def __init__(self, process_name):
        self.process_name = process_name

    def process(self):
        if self.process_name == "download_twitter_profile_image":
            self._download_profile_image_()
        elif self.process_name == "expanded_url":
            self.process_expanded_url()
        elif self.process_name == "official_handles":
            self.process_officials_information()
        elif self.process_name == "paypal_username":
            self.process_user_pay_pal_info()
        elif self.process_name == "timelines_emails":
            self.process_time_lines_found_email()
        elif self.process_name == "telegram_emails":
            self.process_telegram_found_email()
        elif self.process_name == "instagram_emails":
            self.process_instagram_found_email()
        elif self.process_name == "youtube_emails":
            self.process_youtube_found_email()

    def process_youtube_found_email(self):
        _final_email_info = {}
        _mapper = []

        _search_accounts = self.all_search_accounts_from_squatting()
        for each_handle in db_util.MongoDBActor(constants.COLLECTIONS.YOUTUBE_APIFY_SEARCH).find(
                {"text": {"$regex": "email"}}):
            if 'text' in each_handle and 'channelUrl' in each_handle:
                _text = each_handle['text']
                _channel_handle = each_handle['channelUrl']

                _tuple = (_text, _channel_handle)
                if _tuple not in _mapper:
                    print("Found:{}".format(_tuple))
                    _mapper.append(_tuple)

        counter = 1
        for each_map in _mapper:
            _text = each_map[0]
            may_be_email = shared_util.get_email_address_from_line_item(_text)
            if not may_be_email:
                continue
            _text = _text.replace("\n", "").replace(",", "").replace('"', "")
            if may_be_email not in _final_email_info:
                print("Found:{}".format(may_be_email))
                _final_email_info[may_be_email] = {
                    'count': counter,
                    'text': _text
                }
                counter = counter + 1

        with open("report/attributes/paypal_data_share/youtube_emails.json", "w") as f_write:
            json.dump(_final_email_info, f_write, indent=4)

    def process_instagram_found_email(self):
        _final_email_info = {}
        _mapper = []

        _search_accounts = self.all_search_accounts_from_squatting()

        for each_handle in db_util.MongoDBActor(constants.COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).find(
                {"biography": {"$regex": "email"}}):
            if 'biography' in each_handle and 'username' in each_handle:
                _text = each_handle['biography']
                _channel_handle = each_handle['username']

                if _channel_handle.lower() not in _search_accounts:
                    continue
                _tuple = (_text, _channel_handle)
                if _tuple not in _mapper:
                    print("Found:{}".format(_tuple))
                    _mapper.append(_tuple)

        for each_handle in db_util.MongoDBActor(constants.COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).find(
                {"latestIgtvVideos.caption": {"$regex": "email"}}):
            if 'latestIgtvVideos' in each_handle and 'username' in each_handle:
                _videos = each_handle['latestIgtvVideos']
                for v in _videos:
                    if 'caption' in v:
                        _channel_handle = each_handle['username']
                        _text = v['caption']
                        _tuple = (_text, _channel_handle)
                        if _tuple not in _mapper:
                            print("Found:{}".format(_tuple))
                            _mapper.append(_tuple)
        for each_handle in db_util.MongoDBActor(constants.COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).find(
                {"latestPosts.caption": {"$regex": "email"}}):
            if 'latestPosts' in each_handle and 'username' in each_handle:
                _posts = each_handle['latestPosts']
                for v in _posts:
                    if 'caption' in v:
                        _channel_handle = each_handle['username']
                        _text = v['caption']
                        _tuple = (_text, _channel_handle)
                        if _tuple not in _mapper:
                            print("Found:{}".format(_tuple))
                            _mapper.append(_tuple)

        counter = 1
        for each_map in _mapper:
            _text = each_map[0]
            may_be_email = shared_util.get_email_address_from_line_item(_text)
            if not may_be_email:
                continue
            _text = _text.replace("\n", "").replace(",", "").replace('"', "")
            if may_be_email not in _final_email_info:
                print("Found:{}".format(may_be_email))
                _final_email_info[may_be_email] = {
                    'count': counter,
                    'text': _text
                }
                counter = counter + 1

        with open("report/attributes/paypal_data_share/instagram_emails.json", "w") as f_write:
            json.dump(_final_email_info, f_write, indent=4)

    def process_telegram_found_email(self):
        _final_email_info = {}
        _mapper = []

        _search_accounts = self.all_search_accounts_from_squatting()

        for each_handle in db_util.MongoDBActor(constants.COLLECTIONS.TELEGRAM_CHANNELS).find(
                {"info_content": {"$regex": "email"}}):
            if 'info_content' in each_handle and 'channel_handle' in each_handle:
                _info_content_text = each_handle['info_content']
                _channel_handle = each_handle['channel_handle']

                if _channel_handle.lower() not in _search_accounts:
                    continue

                _tuple = (_info_content_text, _channel_handle)
                if _tuple not in _mapper:
                    print("Found:{}".format(_tuple))
                    _mapper.append(_tuple)

        for each_handle in db_util.MongoDBActor(constants.COLLECTIONS.TELEGRAM_APIFY_SEARCH).find(
                {"text": {"$regex": "email"}}):
            if 'text' in each_handle and 'channelName' in each_handle:
                _info_content_text = each_handle['text']
                _channel_handle = each_handle['channelName']

                if _channel_handle.lower() not in _search_accounts:
                    continue

                _tuple = (_info_content_text, _channel_handle)
                if _tuple not in _mapper:
                    print("Found:{}".format(_tuple))
                    _mapper.append(_tuple)
        for each_handle in db_util.MongoDBActor(constants.COLLECTIONS.TELEGRAM_APIFY_SEARCH).find(
                {"linkPreview": {"$regex": "email"}}):
            if 'linkPreview' in each_handle and 'channelName' in each_handle:
                _info_content_text = each_handle['linkPreview']
                _channel_handle = each_handle['channelName']

                if _channel_handle.lower() not in _search_accounts:
                    continue

                _tuple = (_info_content_text, _channel_handle)
                if _tuple not in _mapper:
                    print("Found:{}".format(_tuple))
                    _mapper.append(_tuple)

        counter = 1
        for each_map in _mapper:
            _text = each_map[0]
            may_be_email = shared_util.get_email_address_from_line_item(_text)
            if not may_be_email:
                continue
            _text = _text.replace("\n", "").replace(",", "").replace('"', "")
            if may_be_email not in _final_email_info:
                print("Found:{}".format(may_be_email))
                _final_email_info[may_be_email] = {
                    'count': counter,
                    'text': _text
                }
                counter = counter + 1

        with open("report/attributes/paypal_data_share/telegram_emails.json", "w") as f_write:
            json.dump(_final_email_info, f_write, indent=4)

    def all_search_accounts_from_squatting(self):
        _all_ = set(shared_util.get_typosquatted_search_handle()
                    + shared_util.get_combo_squatted_search_handle() +
                    shared_util.get_fuzzy_squatted_search_handle())
        return _all_

    def process_time_lines_found_email(self):
        _all_emails = {}
        time_lines_username = shared_util.get_all_user_from_timelines()
        len_all_user = len(time_lines_username)
        _search_accounts = self.all_search_accounts_from_squatting()

        for counter, _user in enumerate(time_lines_username):
            if _user.lower() not in _search_accounts:
                continue
            print("Processing {}/{}, user:{}".format(counter, len_all_user, _user))
            _found_emails_on_text = shared_util.get_all_times_lines_containing_text_from_user_name('@gmail.com', _user)
            for _c, _e in enumerate(_found_emails_on_text):
                print("Processing found text: {}/{}, text:{}".format(_c, len(_found_emails_on_text), _e))
                may_be_email = shared_util.get_email_address_from_line_item(_e)
                print("May be email: {}".format(may_be_email))
                if may_be_email:
                    if may_be_email in _all_emails:
                        continue
                    _e = _e.replace("\n", "").replace(",", "").replace('"', "")
                    print("{},{}".format(may_be_email, _e))
                    _all_emails[may_be_email] = _e

        dir_path = "report/attributes/paypal_data_share"
        isExist = os.path.exists(dir_path)
        if not isExist:
            os.makedirs(dir_path)

        with open("{}/timelines_email.csv".format(dir_path), "w") as f_write:
            for _email, _text_ in _all_emails.items():
                f_write.write("{}, {}\n".format(_email, _text_))

    def process_user_pay_pal_info(self):
        _telegram_user_handles = shared_util.get_all_telegram_user_from_db()
        self._process_handle_from_social_media(_telegram_user_handles, 'telegram')

        _twitter_user_handles = shared_util.get_all_user_from_db()
        self._process_handle_from_social_media(_twitter_user_handles, 'twitter')

    def _process_handle_from_social_media(self, _user_handles, _social_media):
        _len_user_handle = len(_user_handles)
        for counter, _user in enumerate(_user_handles):
            print("Processing {}/{} {} from {}".format(counter, _len_user_handle, _user, _social_media))
            is_found = len(set(db_util.MongoDBActor(constants.COLLECTIONS.PAYPAL_INFO).distinct(key="username",
                                                                                                filter={
                                                                                                    "username": _user}))) > 0
            if is_found:
                print("Already processed {}/{} {} from {}".format(counter, _len_user_handle, _user, _social_media))
                continue
            _page_ = PageDriver("https://www.paypal.com/paypalme/{}".format(_user))
            _driver = _page_.get_driver()
            _send_money = _page_.try_waiting_xpath('//*[@id="contents"]/div/div[2]/div/div[3]/a', 3)
            _page_source = _driver.page_source
            if not _send_money:
                db_util.MongoDBActor(constants.COLLECTIONS.PAYPAL_INFO).insert_data({
                    'username': _user,
                    'is_found': False,
                    'page_source': _page_source
                })
                continue

            _user_name_element = _page_.try_waiting_xpath('//*[@id="contents"]/div/div[2]/div/div[2]/div[1]', 3)
            if _user_name_element:
                _user_name_paypal = _user_name_element.text
                _user_name_paypal = _user_name_paypal.strip()
                db_util.MongoDBActor(constants.COLLECTIONS.PAYPAL_INFO).insert_data({
                    'username': _user,
                    'is_found': True,
                    'page_source': _page_source,
                    'name': _user_name_paypal,
                    'social_media': _social_media
                })
                continue
            else:
                db_util.MongoDBActor(constants.COLLECTIONS.PAYPAL_INFO).insert_data({
                    'username': _user,
                    'is_found': False,
                    'page_source': _page_source,
                    'is_error': True,
                    'social_media': _social_media
                })

            _page_.clean_up()

    def process_officials_information(self):
        _data_ = ["Rank, Domain, Twitter Handle, Instagram Handle\n"]
        for val in db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).find():
            if 'twitter' in val:
                _twitter_handle = val['twitter']
                if len(_twitter_handle) > 15:
                    _twitter_handle = 'twit?'
                else:
                    _twitter_handle = _twitter_handle.replace('"', "")
            else:
                _twitter_handle = 'twt?'

            if 'instagram' in val:
                _instagram_handle = val['instagram']
                if len(_instagram_handle) > 30:
                    _instagram_handle = 'inst?'
                else:
                    _instagram_handle = _instagram_handle.replace('"', "")

            else:
                _instagram_handle = 'inst?'

            _data_.append("{},{},{},{}\n".format(val['rank'], val['domain'], _twitter_handle, _instagram_handle))

        dir_path = "report/attributes/official_handles"
        isExist = os.path.exists(dir_path)
        if not isExist:
            os.makedirs(dir_path)

        with open("{}/insta_twitter_handle.csv".format(dir_path), "w") as f_write:
            for _domain_ in _data_:
                f_write.write(_domain_)

    def _download_profile_image_(self):
        _all_screen_names = set(db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).distinct(
            key="username",
            filter={'profile_image_hash': {'$exists': False}}))
        _offical_twitter_accounts = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="twitter",
                                                                                                    filter={
                                                                                                        "is_twitter_candidate": True}))
        _all_screen_names = _all_screen_names.union(_offical_twitter_accounts)
        _len_all_ = len(_all_screen_names)
        for counter, screen_name in enumerate(_all_screen_names):
            print("{}/{}: Processing to download profile image of screen name:{}".format(counter, _len_all_,
                                                                                         screen_name))
            try:
                shared_util.fetch_user_if_not_present(screen_name)
                user_info = None
                for val in db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).find(key={'username': screen_name}):
                    if 'data' in val:
                        user_info = val['data']

                if not user_info:
                    continue
                user_detail = user_info[0]
                if 'profile_image_url' in user_detail:
                    _profile_image_url = user_detail['profile_image_url']
                    f_path = "{}{}.png".format(IMG_DIR_PATH, screen_name)
                    twitter_api.TwitterFeeds().download_image(_profile_image_url, f_path)
                    time.sleep(2)
                    image_hash = shared_util.get_hash_from_image(f_path)
                    _id = db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).find_and_modify(key={
                        'username': screen_name
                    }, data={
                        'profile_image_hash': image_hash,
                        'profile_image_hash_local_path': f_path
                    })
                    print("Inserted profile image:{}, screen_name:{}, hash:{}".format(_profile_image_url, screen_name,
                                                                                      image_hash))
                    time.sleep(1)
            except Exception as ex:
                print("Exception occurred {}".format(ex))

    def _get_unprocessed_expanded_url(self):
        _all_expanded_url = set()
        counter = 0
        for val in db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_TIMELINESS).find({
            "entities.urls.expanded_url": {'$exists': True},
            'expanded_url_processed': {'$exists': False}}):

            counter = counter + 1
            if counter == 5:
                break
            if 'entities' in val:
                if 'urls' in val['entities']:
                    _urls = val['entities']['urls']
                    for _url in _urls:
                        if 'expanded_url' in _url:
                            _all_expanded_url.add(_url['expanded_url'])
        return list(_all_expanded_url)

    def process_expanded_url(self):
        _all_expanded_url_unprocessed = self._get_unprocessed_expanded_url()
        _exclude_cases = ["twitter.", "instagram.", "instagr.", "docs.google.com", "facebook.", "fb.", "youtube.",
                          "youtu."]
        for _url in _all_expanded_url_unprocessed:
            print("Processing screen meta data collect of url: {}".format(_url))
            _avoid_screenshot = False
            for _e in _exclude_cases:
                if _e in _url:
                    _avoid_screenshot = True
                    break

            if not _avoid_screenshot:
                self._take_screen_shot_and_record_meta_data(_url)

            db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_TIMELINESS).find_and_modify(
                key={"entities.urls.expanded_url": _url},
                data={
                    'expanded_url_processed': True,
                    'avoided_screen_shot': _avoid_screenshot
                })

    def _take_screen_shot_and_record_meta_data(self, _url):
        try:
            print(_url)
            page_driver = PageDriver(_url)
            _driver = page_driver.get_driver()
            time.sleep(1.5)
        except Exception as ex:
            print("Unable to process the driver {}".format(ex))
            return
        try:
            _text = _driver.page_source
            print(_text)
            _processed_time = int(time.time() * 1000)

            _str_url_for_hash = "{}{}".format(_url, _processed_time)
            _img_hash = hashlib.sha256(_str_url_for_hash.encode('utf-8')).hexdigest()
            _img_hash = "{}.png".format(_img_hash)
            _location = "IMG_DIR_PATH{}".format(_img_hash)
            _driver.save_screenshot(_location)

            _img_to_text = shared_util.get_text_from_img(_location)
            _driver.close()

            _data = {'last_check': _processed_time, 'img_location_{}'.format(_processed_time): _location,
                     'img_text_{}'.format(_processed_time): _img_to_text}

            db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_EXPANDED_URL).find_and_modify(key={'expanded_url': _url},
                                                                                             data=_data)
            time.sleep(1)

        except Exception as ex:
            print("Exception occurred in processing the screenshot {}".format(ex))
            _driver.close()


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Twitter metadata fetch")
    _arg_parser.add_argument("-p", "--process_name",
                             action="store",
                             required=True,
                             help="processing function name")

    _arg_value = _arg_parser.parse_args()

    meta_data = TwitterAccountMetaDataCollector(_arg_value.process_name)
    meta_data.process()
