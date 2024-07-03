import argparse
import json
import twitter_api
import requests
import glob
import shutil
import os
import time
import shared_util
from bs4 import BeautifulSoup
from db_util import MongoDBActor
from constants import COLLECTIONS


class DataShare:
    def __init__(self, function_name):
        self.function_name = function_name

    def process(self):
        if self.function_name == DataShare.image_infringement_dataset_collect_official_logo.__name__:
            self.image_infringement_dataset_collect_official_logo()
        elif self.function_name == DataShare.image_infringement_dataset_collect_twitter_search_accounts.__name__:
            self.image_infringement_dataset_collect_twitter_search_accounts()
        elif self.function_name == DataShare.image_infringement_dataset_collect_telegram_search_accounts.__name__:
            self.image_infringement_dataset_collect_telegram_search_accounts()
        elif self.function_name == DataShare.image_infringement_dataset_collect_you_tube_search_accounts.__name__:
            self.image_infringement_dataset_collect_you_tube_search_accounts()
        elif self.function_name == DataShare.image_infringement_dataset_collect_instagram_search_accounts.__name__:
            self.image_infringement_dataset_collect_instagram_search_accounts()
        elif self.function_name == DataShare.images_for_data_share_for_image_clustering.__name__:
            self.images_for_data_share_for_image_clustering()
        elif self.function_name == DataShare.you_tube_sample_share.__name__:
            self.you_tube_handle_share()
        elif self.function_name == DataShare.slack_channel_candidate.__name__:
            self.slack_channel_candidate()
        elif self.function_name == DataShare.run_from_local.__name__:
            self.run_from_local()
        elif self.function_name == "collect_instagram_accounts":
            self.collect_instagram_accounts()
        elif self.function_name == "collect_you_tube_accounts":
            self.collect_you_tube_accounts()
        else:
            raise Exception("Unsupported request!")

    def collect_instagram_accounts(self):
        urls = set()
        for val in MongoDBActor(COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).find():
            if 'url' in val:
                urls.add(val['url'])
        with open("report/attributes/instagram_urls/all_db_instagram_urls.txt", "w") as f_write:
            for val in urls:
                f_write.write("{}\n".format(val))

    def collect_you_tube_accounts(self):
        channel_urls = set()
        for val in MongoDBActor(COLLECTIONS.YOUTUBE_APIFY_SEARCH).find():
            if 'channelUrl' in val:
                channel_urls.add(val['channelUrl'])
        with open("report/attributes/you_tube_urls/all_you_tube_db_urls.txt", "w") as f_write:
            for val in channel_urls:
                f_write.write("{}\n".format(val))

    def run_from_local(self):
        with open("report/attributes/candidate_domain_lists/all_found_instagram_data.csv", "r") as f_read:
            lines = f_read.readlines()
        for cnt, line in enumerate(lines):
            if cnt == 0:
                continue
            splitter = line.split(",")
            if len(splitter) < 2:
                continue
            instagram_account = splitter[2]
            instagram_account = instagram_account.strip()
            self.download_instagram_profile_pic(username=instagram_account)
            time.sleep(1)

    def download_instagram_profile_pic(self, username, _download_dir=None):
        if not _download_dir:
            _download_dir = "/Users/bacharya/Desktop/instagram_accounts"
        try:
            # filename
            file_name = "{}/instagram_{}.png".format(_download_dir, username)
            if os.path.exists(file_name):
                print("Already processed, escaping {}".format(file_name))
                return
                # create the URL for the user's Instagram profile
            url = f"https://www.instagram.com/{username}/"
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            # find the  tag containing the URL of the profile picture
            meta_tag = soup.find('meta', attrs={'property': 'og:image'})
            # extract the URL from the 'content' attribute of the  tag
            profile_pic_url = meta_tag['content']
            # send a GET request to the profile picture URL and download the image
            pic_response = requests.get(profile_pic_url)
            with open(file_name, 'wb') as f:
                f.write(pic_response.content)
            print(f"Profile picture for {username} downloaded successfully!")
        except Exception as ex:
            print("Exception occurred {}, username:{}".format(ex, username))

    def image_infringement_dataset_collect_official_logo(self):
        _download_path_dir = "brand_impersonation_data/images/official"
        _official_domains = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="domain", filter={
            "is_candidate": True
        }))

        _exceptions = []
        for _off_domain in _official_domains:
            log_url = shared_util.get_klazify_logo_url_from_domain(_off_domain)
            _domain_name = shared_util.get_name_from_domain(_off_domain)
            download_path = "{}/official_{}.png".format(_download_path_dir, _domain_name)

            if os.path.isfile(download_path):
                print("Already file processed {}".format(download_path))
                continue

            try:
                response = requests.get(log_url, stream=True)
                if response.status_code != 200:
                    raise Exception(response.status_code, response.text)

                if response.status_code == 200:
                    # is_exist = os.path.exists(save_path)
                    # if is_exist:
                    #     os.makedirs(save_path)
                    with open('{}'.format(download_path), 'wb') as f:
                        response.raw.decode_content = True
                        shutil.copyfileobj(response.raw, f)
            except Exception as ex:
                _msg = ("Exception occurred in image fetch ex:{}, domain name:{}, _off_domain:{}, url:{}"
                        .format(ex, _domain_name, _off_domain, log_url))
                _exceptions.append(_msg)
                print(_msg)

            print("logo url:{}, "
                  "Domain name:{},"
                  "Image downloaded:{}".format(log_url, _domain_name, download_path))
            time.sleep(1)

        for cnt, val in enumerate(_exceptions):
            print("{}, {}".format(cnt, val))

    def image_infringement_dataset_collect_twitter_search_accounts(self):
        _download_path_dir = "brand_impersonation_data/images/twitter"
        _official_domains = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="domain", filter={
            "is_twitter_candidate": True
        }))

        _official_twitter_accounts = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="twitter", filter={
            "is_twitter_candidate": True
        }))

        _all_found_usernames = set()
        for _off_domain in _official_domains:
            if not _off_domain:
                continue
            _domain_name = shared_util.get_name_from_domain(_off_domain)
            _found_usernames = shared_util.get_twitter_found_usernames_from_domain_name(_domain_name)
            _all_found_usernames = _all_found_usernames.union(_found_usernames)
        # make sure this found username exclude officials
        _all_found_usernames = _all_found_usernames.difference(_official_twitter_accounts)
        if None in _all_found_usernames:
            _all_found_usernames.remove(None)
        for _username in _all_found_usernames:
            _src_img = "/home/bacharya/usernameSquat_images/{}.png".format(_username)
            _des_img = "{}/twitter_{}.png".format(_download_path_dir, _username)

            # check if file is already processed
            if os.path.isfile(_des_img):
                print("Already file processed {}".format(_des_img))
                continue

            # check image exists
            if os.path.isfile(_src_img):
                shutil.copy(_src_img, _des_img)
                print("Copied image:{}, {}".format(_src_img, _des_img))

    # note this code is run after copying the images from user_name_squat/images/ to final_images
    # this is to ensure that search account does not include official account
    def images_for_data_share_for_image_clustering(self):
        _official_twitter_accounts = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="instagram", filter={
            "is_twitter_candidate": True
        }))
        self.delete_official_file_from_non_official_folder(_official_twitter_accounts,
                                                           "brand_impersonation_data"
                                                           "/cluster_images/twitter/twitter")

        _official_instagram_accounts = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="instagram", filter={
            "is_instagram_candidate": True
        }))

        self.delete_official_file_from_non_official_folder(_official_instagram_accounts,
                                                           "brand_impersonation_data"
                                                           "/cluster_images/instagram/instagram")

        _official_youtube_accounts = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="instagram", filter={
            "is_youtube_candidate": True
        }))

        self.delete_official_file_from_non_official_folder(_official_youtube_accounts,
                                                           "brand_impersonation_data"
                                                           "/cluster_images/youtube/youtube")

    def delete_official_file_from_non_official_folder(self, official_accounts, search_text_path):
        _len = len(official_accounts)
        for cnt, acc in enumerate(official_accounts):
            if acc is None:
                continue
            f_path = "{}_{}.png".format(search_text_path, acc)
            print("Processing:{}/{}".format(cnt, _len, f_path))
            try:
                if os.path.isfile(f_path):
                    os.remove(f_path)
            except Exception:
                print("File does not exists")

    def image_infringement_dataset_collect_instagram_search_accounts(self):
        _download_path_dir = "brand_impersonation_data/images/instagram"
        _official_domains = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="domain", filter={
            "is_instagram_candidate": True
        }))

        _official_instagram_accounts = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="instagram", filter={
            "is_instagram_candidate": True
        }))

        ####
        # All of the two cases II and III did not work, because of the expired url signature for profile pic
        # so we call apfiy api again to fetch the userprofile data using scrape_single_profile_api
        # so this results below contains
        # first found account names from instagram_post_search
        # and second data contains related accounts from found search profile
        # Thus this yield more profile data and picture
        ###
        # Case I:

        _to_process_image_download = []
        instagram_single_profile_data = set(
            MongoDBActor(COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).distinct(key="username"))
        for each_profile in instagram_single_profile_data:
            _found_profile_pic = set(
                MongoDBActor(COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).distinct(key="profilePicUrl", filter={
                    "username": each_profile
                }))

            if None in _found_profile_pic:
                _found_profile_pic.remove(None)

            if len(_found_profile_pic) < 1:
                continue
            _profile_pic = list(_found_profile_pic)[0]
            _to_process_image_download.append((each_profile, _profile_pic))
            print("main", each_profile, _profile_pic)
            for val in MongoDBActor(COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).find({"username": each_profile}):
                if 'relatedProfiles' in val:
                    _related_profiles = val['relatedProfiles']
                    for _rp in _related_profiles:
                        if 'username' in _rp and 'profile_pic_url' in _rp:
                            _to_process_image_download.append((_rp['username'], _rp['profile_pic_url']))
                            print("related", _rp['username'], _rp['profile_pic_url'])

        for _profile_data in _to_process_image_download:
            username = _profile_data[0]
            profile_image_url = _profile_data[1]
            download_path = "{}/instagram_{}.png".format(_download_path_dir, username)

            if os.path.isfile(download_path):
                print("Already instagram file processed {}".format(download_path))
                continue

            try:
                response = requests.get(profile_image_url, stream=True)
                if response.status_code != 200:
                    raise Exception(response.status_code, response.text)

                if response.status_code == 200:
                    # is_exist = os.path.exists(save_path)
                    # if is_exist:
                    #     os.makedirs(save_path)
                    with open('{}'.format(download_path), 'wb') as f:
                        response.raw.decode_content = True
                        shutil.copyfileobj(response.raw, f)
            except Exception as ex:
                _msg = ("Exception occurred in image fetch ex:{}, usernme:{}, url:{}"
                        .format(ex, username, profile_image_url))
                print(_msg)

            print("logo url:{}, "
                  "Instagram name:{},"
                  "Image downloaded:{}".format(profile_image_url, username, download_path))
            time.sleep(0.25)

        ##########
        return  # the below functions are useless but kept for general information
        ########
        # ----------------------------------------------------------

        # Case II: Use beautiful soap to fetch the profile pic
        _len = len(_official_domains)
        for cnt, _off_domain in enumerate(_official_domains):
            print("Processing beautiful soap:{}/{}, {} ".format(cnt, _len, _off_domain))
            if not _off_domain:
                continue
            domain_name = shared_util.get_name_from_domain(_off_domain)
            _found_accounts = shared_util.get_instagram_found_usernames_from_domain_name(domain_name)
            if None in _found_accounts:
                _found_accounts.remove(None)
            for account_name in _found_accounts:
                # exclude official accounts
                if account_name in _official_instagram_accounts:
                    continue
                self.download_instagram_profile_pic(account_name)
                time.sleep(0.25)

        # ------------------------------------------------------------------------------

        # Case III: Using potentially expired profilePicurl, not reliable method
        # Instagram has a signature expired to its profile pic
        _all_found_usernames = set()
        for _off_domain in _official_domains:
            if not _off_domain:
                continue
            domain_name = shared_util.get_name_from_domain(_off_domain)
            _found_usernames = shared_util.get_instagram_found_usernames_from_domain_name(domain_name)
            _all_found_usernames = _all_found_usernames.union(_found_usernames)

        if None in _all_found_usernames:
            _all_found_usernames.remove(None)

        # exclude official account
        _all_found_usernames = _all_found_usernames.difference(_official_instagram_accounts)
        _all_len = len(_all_found_usernames)
        for cnt, _username in enumerate(_all_found_usernames):
            log_url = shared_util.get_instagram_profile_pic_url_from_user_name(_username)
            download_path = "{}/instagram_{}.png".format(_download_path_dir, _username)

            print("Processing:{}/{}, {},{}".format(cnt, _all_len, log_url, download_path))
            # check if file is already processed
            if os.path.isfile(download_path):
                print("Already file processed {}".format(download_path))
                continue
            try:
                response = requests.get(log_url, stream=True)
                if response.status_code != 200:
                    raise Exception(response.status_code, response.text)

                if response.status_code == 200:
                    # is_exist = os.path.exists(save_path)
                    # if is_exist:
                    #     os.makedirs(save_path)
                    with open('{}'.format(download_path), 'wb') as f:
                        response.raw.decode_content = True
                        shutil.copyfileobj(response.raw, f)
            except Exception as ex:
                print("Exception occurred in instagram image fetch {}".format(ex))

            print("logo url:{}, "
                  "Image downloaded:{}".format(log_url, download_path))
            time.sleep(0.25)

    def image_infringement_dataset_collect_telegram_search_accounts(self):
        _download_path_dir = "brand_impersonation_data/images/telegram"
        _official_domains = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="domain", filter={
            "is_candidate": True
        }))

        _all_found_usernames = set()
        for _off_domain in _official_domains:
            if not _off_domain:
                continue
            domain_name = shared_util.get_name_from_domain(_off_domain)
            _found_usernames = shared_util.get_telegram_found_usernames_from_domain_name(domain_name)
            _all_found_usernames = _all_found_usernames.union(_found_usernames)

        if None in _all_found_usernames:
            _all_found_usernames.remove(None)

        _all_len = len(_all_found_usernames)
        for cnt, _username in enumerate(_all_found_usernames):
            log_url = shared_util.get_telegram_profile_pic_url_from_channel_handle(_username)
            download_path = "{}/telegram_{}.png".format(_download_path_dir, _username)

            print("Processing:{}/{}, {},{}".format(cnt, _all_len, log_url, download_path))
            # check if file is already processed
            if os.path.isfile(download_path):
                print("Already file processed {}".format(download_path))
                continue
            try:
                response = requests.get(log_url, stream=True)
                if response.status_code != 200:
                    raise Exception(response.status_code, response.text)

                if response.status_code == 200:
                    # is_exist = os.path.exists(save_path)
                    # if is_exist:
                    #     os.makedirs(save_path)
                    with open('{}'.format(download_path), 'wb') as f:
                        response.raw.decode_content = True
                        shutil.copyfileobj(response.raw, f)
            except Exception as ex:
                print("Exception occurred in telegram image fetch {}".format(ex))

            print("logo url:{}, "
                  "Image downloaded:{}".format(log_url, download_path))
            time.sleep(0.25)

    def image_infringement_dataset_collect_you_tube_search_accounts(self):
        _download_path_dir = "brand_impersonation_data/images/youtube"
        _official_domains = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="domain", filter={
            "is_youtube_candidate": True
        }))

        _official_youtube = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="youtube", filter={
            "is_youtube_candidate": True
        }))

        print(list(_official_youtube))

        _all_usernames = set()
        for counter, _off_domain in enumerate(_official_domains):
            print("Processing {}/{}, {}".format(counter, len(_official_domains), _off_domain))
            if not _off_domain:
                continue
            domain_name = shared_util.get_name_from_domain(_off_domain)
            _found_usernames = shared_util.get_youtube_found_usernames_from_domain_name(domain_name)
            if len(_found_usernames) > 0:
                _all_usernames = _all_usernames.union(_found_usernames)

        if None in _all_usernames:
            _all_usernames.remove(None)
        for cnt, _channel_url in enumerate(_all_usernames):
            thumbnail_url = shared_util.get_youtube_thumb_nail_url_from_channel_url(_channel_url)
            if not thumbnail_url:
                print("Thumbnail absent:{}".format(_channel_url))
                continue
            _handle = _channel_url

            print("counter:{}/{}, {}".format(cnt, _channel_url,
                                             thumbnail_url))

            if "http://www.youtube.com/channel/" in _handle:
                _handle = _handle.replace("http://www.youtube.com/channel/", "")
            elif "http://www.youtube.com/" in _handle:
                _handle = _handle.replace("http://www.youtube.com/", "")
            elif "https://www.youtube.com/" in _handle:
                _handle = _handle.replace("https://www.youtube.com/", "")
            elif "http://youtube.com/" in _handle:
                _handle = _handle.replace("http://youtube.com/", "")
            elif "https://youtube.com/" in _handle:
                _handle = _handle.replace("https://youtube.com/", "")

            _handle = _handle.strip()
            if _handle.startswith("@"):
                _handle = _handle[1:]

            if "youtube/" in _handle:
                _handle = _handle.replace("youtube/", "")

            if not _handle:
                continue
            # exclude official handle
            if _handle in _official_youtube:
                continue

            download_path = "{}/youtube_{}.png".format(_download_path_dir, _handle)
            # check if file is already processed
            if os.path.isfile(download_path):
                print("Already file processed {}".format(download_path))
                continue
            try:
                response = requests.get(thumbnail_url, stream=True)
                if response.status_code != 200:
                    raise Exception(response.status_code, response.text)

                if response.status_code == 200:
                    # is_exist = os.path.exists(save_path)
                    # if is_exist:
                    #     os.makedirs(save_path)
                    with open('{}'.format(download_path), 'wb') as f:
                        response.raw.decode_content = True
                        shutil.copyfileobj(response.raw, f)
            except Exception as ex:
                print("Exception occurred in telegram image fetch {}".format(ex))

            print("logo url:{}, "
                  "Image downloaded:{}".format(thumbnail_url, download_path))
            time.sleep(0.25)

    def you_tube_handle_share(self):
        _official = {}
        for val in MongoDBActor("domain").find():
            if "is_youtube_candidate" not in val:
                continue
            _official[val['name']] = val['youtube']

        _all_data = {}
        for val in MongoDBActor("youtube_account_search").find():
            if 'channelUrl' in val and 'fromYTUrl' in val:
                _channel_url = val['channelUrl']
                _search_result = val['fromYTUrl']
                print(_channel_url, _search_result)

                if not _channel_url:
                    continue
                if not _search_result:
                    continue
                _channel_url = _channel_url.replace("http://www.youtube.com/@", "")
                _channel_url = _channel_url.replace("https://www.youtube.com/@", "")
                _channel_url = _channel_url.replace("http://www.youtube.com/", "")
                _channel_url = _channel_url.replace("https://www.youtube.com/", "")
                _channel_url = _channel_url.replace("channel/", "")

                _search_result = _search_result.replace("https://www.youtube.com/results?search_query=", "")

                if _search_result not in _all_data:
                    _all_data[_search_result] = [_channel_url]
                else:
                    _prev = _all_data[_search_result] + [_channel_url]
                    _all_data[_search_result] = list(set(_prev))

        print(_official)
        for _key, _values in _all_data.items():
            if len(_values) == 0:
                continue
            if _key not in _official:
                continue
            _fname = _official[_key]
            with open("report/attributes/data_share/you_tube_handle/{}.txt".format(_fname), "w") as f_write:
                for _v in _values:
                    f_write.write("{}\n".format(_v))

    def you_tube_sample_share(self):
        _data_ = []
        for val in MongoDBActor("youtube_account_search").find(
                {"fromYTUrl": "https://www.youtube.com/results?search_query=paypal"}):
            found_data = val
            del found_data["_id"]
            del found_data["domain"]
            del found_data["domain_name"]
            print(found_data)
            if found_data not in _data_:
                _data_.append(found_data)
        with open("report/attributes/data_share/you_tube/paypal_search_channels.json", "w") as f_write:
            json.dump(_data_, f_write, indent=4)

    # keyword search for Enrico
    def slack_channel_candidate(self):
        search_keyword = []
        for i in range(1, 10000):
            for val in MongoDBActor(COLLECTIONS.DOMAIN).find({"rank": i, "is_candidate": True}):
                if 'name' in val:
                    _found = val['name']
                    if _found not in search_keyword:
                        search_keyword.append(_found)
        with open("report/attributes/data_share/slack/slack_search.keyword.txt", "w") as f_write:
            for val in search_keyword:
                f_write.write("{}\n".format(val))

    def official_you_tube_candidate(self):
        search_keyword = ["Domain, YouTube Username Handle"]
        for i in range(1, 10000):
            for val in MongoDBActor(COLLECTIONS.DOMAIN).find({"rank": i, "is_youtube_candidate": True}):
                search_keyword.append("{},{}".format(val['domain'], val['youtube']))
        with open("report/attributes/data_share/you_tube/official_handle.csv", "w") as f_write:
            for val in search_keyword:
                f_write.write("{}\n".format(val))


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Process data share")
    _arg_parser.add_argument("-f", "--function_name",
                             action="store",
                             required=True,
                             help="Data collect")

    _arg_value = _arg_parser.parse_args()

    _data_share = DataShare(_arg_value.function_name)
    _data_share.process()
