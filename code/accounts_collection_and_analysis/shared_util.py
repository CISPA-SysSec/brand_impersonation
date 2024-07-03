import constants
import rstr
import time
import hashlib
import tempfile as tmp
import os
import subprocess
import socket
import urllib.request
import requests

from db_util import MongoDBActor
from constants import COLLECTIONS
from twitter_api import TwitterFeeds
from cymruwhois import Client
from ipwhois import IPWhois
from warnings import filterwarnings

filterwarnings(action="ignore")


def get_youtube_found_usernames_from_domain_name(domain_name):
    _channel_handle = set(MongoDBActor(COLLECTIONS.YOUTUBE_APIFY_SEARCH)
                          .distinct(key="channelUrl", filter={"domain_name": domain_name}))
    if None in _channel_handle:
        _channel_handle.remove(None)
    return _channel_handle


def get_url_from_line(line):
    _delim = [":", "___", "💊", "***", "➡️", "🤩", "✅",
              "•", ".e...", "▶️", "====", "———", "Q►", "❤️", "⭐", "▬▬▬", ".ℹ️",
              "😎", "想出去玩", "❗️❗️❗️"]
    try:
        splitter = line.split(" ")
        for each in splitter:
            if "http://" in each:
                each = each.replace("\n", "")
                each = each.strip()
            if "https://" in each:
                each = each.replace("\n", "")
                each = each.strip()
            for p in _delim:
                if p in each:
                    each = each.split(p)
                    for sp in each:
                        if "https://" in sp:
                            return sp.strip()
            if "https://" in each:
                return each
    except:
        pass
    return None


def get_youtube_handle_from_channel_url(channel_url):
    _handle = channel_url
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
    return _handle


def get_telegram_found_usernames_from_domain_name(domain_name):
    _channel_handle = set(MongoDBActor(COLLECTIONS.TELEGRAM_CHANNELS)
                          .distinct(key="channel_handle", filter={"search_text": domain_name}))
    if None in _channel_handle:
        _channel_handle.remove(None)
    return _channel_handle


def get_telegram_profile_pic_url_from_channel_handle(channel_handle):
    for val in MongoDBActor(COLLECTIONS.TELEGRAM_CHANNELS).find({"channel_handle": channel_handle}):
        if 'logo_link' in val:
            _found_logo_link = val['logo_link']
            if _found_logo_link:
                return _found_logo_link
    return None


def get_youtube_thumb_nail_url_from_channel_url(channelUrl):
    for val in MongoDBActor(COLLECTIONS.YOUTUBE_APIFY_SEARCH).find({"channelUrl": channelUrl}):
        if 'thumbnailUrl' in val:
            _found_thumb_nail_link = val['thumbnailUrl']
            if _found_thumb_nail_link:
                return _found_thumb_nail_link
    return None


def get_instagram_found_ownerID_from_domain_name(domain_name):
    _all_found_tc = set()
    _latest_posts_owner_id = set(MongoDBActor(COLLECTIONS.INSTAGRAM_ACCOUNT_SEARCH)
                                 .distinct(key="latestPosts.ownerId", filter={"domain_name": domain_name}))
    _top_posts_owner_id = set(MongoDBActor(COLLECTIONS.INSTAGRAM_ACCOUNT_SEARCH)
                              .distinct(key="topPosts.ownerId", filter={"domain_name": domain_name}))
    _all_found_owner_id = _latest_posts_owner_id.union(_top_posts_owner_id)
    if None in _all_found_owner_id:
        _all_found_owner_id.remove(None)
    return _all_found_owner_id


def get_instagram_found_usernames_from_domain_name(domain_name):
    _names = set(MongoDBActor(COLLECTIONS.INSTAGRAM_ACCOUNT_SEARCH)
                 .distinct(key="name", filter={"domain_name": domain_name}))
    for each_name in _names:
        _related_profiles = get_related_single_profile_from_instagram_search(each_name)
        _names = _names.union(_related_profiles)

    if None in _names:
        _names.remove(None)
    return _names


def get_related_single_profile_from_instagram_search(username):
    _related_profiles = set(MongoDBActor(COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).distinct(
        key="relatedProfiles.username",
        filter={"username": username}))

    if None in _related_profiles:
        _related_profiles.remove(None)
    return _related_profiles


def get_social_media_handle_from_domain(domain, social_media):
    if social_media == "twitter":
        _flag = "twitter"
    elif social_media == "instagram":
        _flag = "instagram"
    elif social_media == "youtube":
        _flag = "youtube"
    elif social_media == "telegram":
        _flag = "name"
    else:
        raise Exception("unsupported social media!")
    for val in MongoDBActor(COLLECTIONS.DOMAIN).find({"domain": domain}):
        if _flag in val:
            return val[_flag]
    return None


def get_instagram_profile_pic_url_from_user_name(domain_name):
    _profile_pic = set(MongoDBActor(COLLECTIONS.INSTAGRAM_ACCOUNT_SEARCH)
                       .distinct(key="profilePicUrl", filter={"domain_name": domain_name}))
    if None in _profile_pic:
        _profile_pic.remove(None)
    return _profile_pic


def get_twitter_found_usernames_from_domain_name(domain_name):
    _all_found_tc = set(MongoDBActor(COLLECTIONS.TWITTER_DAILY_DOMAIN_SEARCH).distinct(key="usernames", filter={
        "domain": domain_name
    }))

    if None in _all_found_tc:
        _all_found_tc.remove(None)
    return _all_found_tc


def is_two_ld_present_in_external_fiterlists_db(two_tld):
    _found_domain_tld = set(MongoDBActor(constants.COLLECTIONS.DOMAIN,
                                         "filterlists").distinct(key="tld_info.name",
                                                                 filter={
                                                                     "tld_info.name": two_tld}))

    if None in _found_domain_tld:
        _found_domain_tld.remove(None)

    return len(_found_domain_tld) > 0


def get_candidate_curated_domains():
    _candidates = set()
    _categories = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="klazify_data.domain.categories.name"))
    for counter, _category in enumerate(_categories):
        _data_append = []
        _domains = MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="domain",
                                                             filter={
                                                                 "klazify_data.domain.categories.name": _category
                                                             })
        for _domain in _domains:
            _url_status_code = get_domain_status_from_db(_domain)
            if _url_status_code not in [200, 201, 202, 203, 204]:
                continue
            _candidates.add(_domain)
    return _candidates


def get_you_tube_link_from_line_item(line_item):
    _potential_scam_search_text = ["youtu.be/", "youtube.com/"]
    return _has_potential_scam_type_search(line_item, _potential_scam_search_text)


def _has_potential_scam_type_search(line_item, _potential_scam_search_text):
    if not line_item:
        return None
    if len(line_item) < 7:
        return None

    line_item = line_item.strip()

    _splitter = line_item.split(" ")
    for _split in _splitter:
        _split = _split.strip()
        if "https://" in _split or "http://" in _split:
            _link = _get_form_full_url(_split)
            if _link is not None:
                for _pt in _potential_scam_search_text:
                    if _pt in _link:
                        return _link
    return None


def _get_form_full_url(splitted):
    # cleanup
    if "\n" in splitted:
        _split_again = splitted.split("\n")
        _potential_url = None
        for _s in _split_again:
            if "https://" in _s:
                _potential_url = _s
                break
        if not _potential_url:
            return None
        splitted = _potential_url

    # safe split
    _split_last = splitted.split("https://")
    if len(_split_last) < 2:
        return None

    reconstruct = "https://{}".format(_split_last[1])
    _splitted = reconstruct

    _remove_additional_end_words = [",", "."]

    for _s in _remove_additional_end_words:
        if _splitted.endswith(_s):
            _splitted = _splitted[0:len(_splitted) - 1]

    if "..." in _splitted:
        _splitted = _splitted.split("...")[0]

    # get final landing url
    full_url = _get_final_landing_url_from_short_url(_splitted)

    if not full_url:
        return None

    if "https://" in full_url:
        splitter = full_url.split("https://")[1]
        return "https://{}".format(splitter)
    elif "http://" in full_url:
        splitter = full_url.split("http://")[1]
        return "http://{}".format(splitter)

    return full_url


def _get_final_landing_url_from_short_url(url):
    try:
        _initial = url
        r = requests.head(url, allow_redirects=False)
        if 300 < r.status_code < 400:
            _final = r.headers.get('Location', url)
            return _final
        else:
            return r.url

    except Exception as ex:
        pass
    return None


def include_categories():
    return [
        "Arts & Entertainment",
        "Business & Industrial",
        "Computers & Electronics",
        "Finance",
        "Internet & Telecom",
        "Online Communities",
        "Shopping",
        "Sports",
        "Travel"
    ]


def may_be_email_address_address_in_page_source(page_source, domain):
    _potential_domains = set()
    _splitter = page_source.split(' ')
    _cleaned_email = None
    for word in _splitter:
        _line_item = "@{}".format(domain)
        if _line_item in word:
            _cleaned_email = clean_line_item_containing_email(word, domain)
            if _cleaned_email:
                _potential_domains.add(_cleaned_email)
    return list(_potential_domains)


def clean_line_item_containing_email(line_item, _domain):
    _keywords = ["<em>", "</em>", "</b>", "\n", ",", "\\x3e", "\\x3c", "<b>", "\\", "(", ")"]
    _email = None
    for _key in _keywords:
        if _key in line_item:
            _splitter = line_item.split(_key)
            for _s in _splitter:
                if _domain in _s:
                    line_item = _s

    line_item = "{}{}".format(line_item.split(_domain)[0], _domain)
    return line_item


def get_domain_status(domain):
    try:
        _code = urllib.request.urlopen(domain, timeout=15).getcode()
        return _code
    except:
        pass

    try:
        _code = requests.head(domain, timeout=15)
        return _code.status_code
    except:
        pass

    return -1


def get_domain_content_category(domain):
    _found_code = list(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="url_status_code", filter={"domain": domain}))
    if len(_found_code) == 0:
        return -99
    return _found_code[0]


def get_domain_status_from_db(domain):
    _found_code = list(
        MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="klazify_data.domain.categories.name", filter={"domain": domain}))
    return _found_code


def get_klazify_logo_url_from_domain(domain):
    for val in MongoDBActor(COLLECTIONS.DOMAIN).find({"domain": domain}):
        if "klazify_data" in val:
            _klazify_data = val['klazify_data']
            if 'domain' in _klazify_data:
                _domain_data = _klazify_data['domain']
                if 'logo_url' in _domain_data:
                    _logo_url = _domain_data['logo_url']
                    if _logo_url:
                        return _logo_url
    return None


def get_domain_url_status_code(domain):
    for val in MongoDBActor(COLLECTIONS.DOMAIN).find({"domain": domain}):
        if "url_status_code" in val:
            return val['url_status_code']
    return -1


def is_domain_in_include_status_code(domain):
    _status_code = get_domain_url_status_code(domain)
    if 200 <= _status_code < 400:
        return True
    return False


def is_domain_in_include_web_content_category(domain):
    _accepted_categories = include_categories()
    _found_categories = get_domain_status_from_db(domain)
    for _f in _found_categories:
        for _ac in _accepted_categories:
            if _ac in _f:
                return True
    return False


def get_domain_registry_info(domain):
    _data = {}
    try:
        ip = socket.gethostbyname(domain)
        c = Client()
        r = c.lookup(ip)
        if 'asn' in r:
            _data['asn'] = r.asn
        if 'cc' in r:
            _data['cc'] = r.cc
        if 'owner' in r:
            _data['owner'] = r.owner
        if 'prefix' in r:
            _data['prefix'] = r.prefix
        _data['ip'] = ip
        obj = IPWhois(ip)
        _data = obj.lookup_whois()
        res = obj.lookup_whois()
        return res
    except Exception as ex:
        print("Exception occurred {}".format(ex))
    return {}


def get_all_domain_url():
    return list(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="domain"))


def get_all_domain_name():
    return list(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="name"))


def get_domain_from_name(name):
    return list(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="domain", filter={"name": name}))[0]


def get_domain_name_from_social_media_handle(social_media, _handle):
    for val in MongoDBActor(COLLECTIONS.DOMAIN).find({social_media: "/^{}$/i".format(_handle)}):
        if 'name' in val:
            return val['name']
    return None


def get_name_from_domain(domain):
    print(domain)
    return list(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="name", filter={"domain": domain}))[0]


def get_pay_pal_accounts():
    return list(MongoDBActor(COLLECTIONS.PAYPAL_INFO).distinct(key="username", filter={"is_found": True}))


def get_account_name_from_pay_pal_user_name(username):
    for val in MongoDBActor(COLLECTIONS.PAYPAL_INFO).find({"username": username}):
        if 'name' in val:
            return val['name']
    return None


def get_twitter_domain_search_username_search_info(username):
    _data = set(MongoDBActor(COLLECTIONS.TWITTER_DAILY_DOMAIN_SEARCH).distinct(key="domain",
                                                                               filter={
                                                                                   "usernames": {"$in": [username]}}))

    if len(_data) == 0:
        return '?'
    _domain_name = _data[0]
    return get_domain_from_name(_domain_name)


def search_append_keywords():
    _keywords = ["recover", "hack", "support", "help", "assist", "contact", "team"]
    return _keywords


def get_usernames_from_daily_twitter_search(domain_name):
    _all_users = []
    _construct_keywords = [domain_name]
    for _k in search_append_keywords():
        _construct_keywords = "{} {}".format(domain_name, _k)

    for _c in _construct_keywords:
        for val in MongoDBActor(COLLECTIONS.TWITTER_DAILY_DOMAIN_SEARCH).find(
                {"search_keyword": _c}):
            if 'usernames' in val:
                _all_users = _all_users + list(val['usernames'])
    return list(set(_all_users))


def get_usernames_from_you_tube_search():
    _all_data = {}
    with open("report/attributes/candidate_domain_lists/all_found_youtube_data.csv", "r") as f_read:
        lines = f_read.readlines()

    for counter, val in enumerate(lines):
        if counter == 0:
            continue
        _splitter = val.split(",")
        _domain = _splitter[0].strip()
        _you_tube = _splitter[1].strip()
        _all_data[_domain] = _you_tube

    _domains = list(_all_data.keys())
    _names = []
    for _d in _domains:
        _users = get_name_from_domain(_d)
        _names.append(_users)

    _names = list(set(_names))
    _to_return = {}
    for _name in _names:
        _all_handle = []
        for val in MongoDBActor(COLLECTIONS.YOUTUBE_APIFY_SEARCH).find({"domain_name": _name}):
            if 'channelUrl' in val:
                _found_url = val['channelUrl']
                _found_url = _found_url.split("@")[1].strip()
                _all_handle.append(_found_url)
        _to_return[_name] = list(set(_all_handle))

    return _to_return


def get_usernames_from_telegram_search(domain):
    _all_users = []
    _name = get_name_from_domain(domain)
    for val in MongoDBActor(COLLECTIONS.TELEGRAM_CHANNELS).find({"search_text": _name}):
        if 'channel_handle' in val:
            _all_users = _all_users + list(val['channel_handle'])
    return list(set(_all_users))


def get_all_telegram_domain_users():
    _all_users = {}
    all_name = list(MongoDBActor(COLLECTIONS.TELEGRAM_CHANNELS).distinct(key="search_text"))
    if None in all_name:
        all_name.remove(None)
    if "" in all_name:
        all_name.remove("")
    for _name in all_name:
        _found = []
        for val in MongoDBActor(COLLECTIONS.TELEGRAM_CHANNELS).find({"search_text": _name}):
            if 'channel_handle' in val:
                _found = _found.append(val['channel_handle'])
        _all_users[_name] = list(set(_found))
    return _all_users


def get_rank_from_domain(domain):
    for val in MongoDBActor(COLLECTIONS.DOMAIN).find({"domain": domain}):
        if 'rank' in val:
            return val['rank']
    return None


def get_rank_from_domain_name(domain_name, _type_='smallest'):
    _found = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="rank", filter={"name": domain_name}))
    print(_found)
    if len(_found) > 0:
        _found = list(_found)
        _found.sort(reverse=True)
        if _type_ == 'highest':
            return _found[0]
        elif _type_ == 'smallest':
            return _found[-1]
    return -1


def get_all_times_lines_text():
    _text_found = set()
    for val in MongoDBActor(COLLECTIONS.TWITTER_TIMELINESS).find():
        if 'text' in val:
            _text_found.add(val['text'])
    return _text_found


def get_all_times_lines_containing_text(_text_):
    _text_found = set()
    for val in MongoDBActor(COLLECTIONS.TWITTER_TIMELINESS).find({"text": {'$regex': _text_}}):
        if 'text' in val:
            _text_found.add(val['text'])
    return _text_found


def get_all_times_lines_containing_text_from_user_name(_text_, username):
    _data_ = set(MongoDBActor(COLLECTIONS.TWITTER_TIMELINESS).distinct(key="text", filter={"text": {'$regex': _text_},
                                                                                           "username": username}))
    if None in _data_:
        _data_.remove(None)
    return list(_data_)


def get_all_times_lines_text_from_user_name(username):
    _text_found = set()
    try:
        for val in MongoDBActor(COLLECTIONS.TWITTER_TIMELINESS).find({"username": username}):
            if 'text' in val:
                _text_found.add(val['text'])
    except:
        print("Exception occured ...username:{}, total text:{}".format(username, len(_text_found)))
        pass
    return _text_found


def get_all_times_lines_url_from_user_name(username):
    _distinct_urls = set(MongoDBActor(COLLECTIONS.TWITTER_TIMELINESS).distinct(key="entities.urls.unwound_url", filter={
        "username": username}))
    return _distinct_urls


def get_all_twitter_times_lines_url_from_user_name_containing_you_tube_link(username):
    _candidates = []
    _urls = get_all_times_lines_url_from_user_name(username)
    for _u in _urls:
        if 'youtube' in _u:
            _candidates.append(_u)
    return _candidates


def isascii(s):
    return len(s) == len(s.encode())


def get_email_address_from_line_item(line_item):
    if not line_item:
        return None
    if len(line_item) < 7:
        return None

    # 14 popular mail clients
    # https://www.mailmunch.com/blog/best-email-service-providers
    _check_email_ = ["@gmail.com", "@mail.com", "@hotmail.com", "@outlook.com", "@aol.com", "@aim.com", "@yahoo.com",
                     "@icloud.com", "@protonmail.com", "@pm.com", "@zoho.com", "@yandex.com", "@titan.com", "@gmx.com",
                     "@hubspot.com", "@tutanota.com"]

    _has_email_client = False
    _email_client = None
    for _e_client in _check_email_:
        if _e_client in line_item:
            _has_email_client = True
            _email_client = _e_client
            break

    if not _has_email_client:
        return None

    _splitter = line_item.split(" ")
    for _split in _splitter:
        _split = _split.strip()
        if _email_client in _split:
            _cleaned_text = _get_cleaned_email_text(_split, _email_client)
            return _cleaned_text
    return None


def _get_cleaned_email_text(_split_text, _email_client):
    # check prepended text in the beginning of the email
    _pre_pended_char = ["&lt;", "@", "\n", ":", ",", "[", "{", "("]
    for _ch in _pre_pended_char:
        if _split_text.startswith(_ch):
            _split_text = _split_text.split(_ch)[1]
            _split_text = _split_text.strip()

    # check ending extra words with email client
    _extra_append_char = [".", "&gt;", ",", "*", "\n", "]", "}", ")"]
    for _ch in _extra_append_char:
        _extra = "{}{}".format(_email_client, _ch)
        if _extra in _split_text:
            _split_text = _split_text.split(_extra)[0]
            # re-add the email_client that removed from above split
            if _email_client not in _split_text:
                _split_text = "{}{}".format(_split_text, _email_client)
                _split_text = _split_text.strip()

    # middle text if present
    _middle_char = ["(", "{", "[", ":", "#", "mail=", "…", "*"]
    for _ch in _middle_char:
        if _ch in _split_text:
            _split_text = _split_text.split(_ch)[1]
            _split_text = _split_text.strip()

    # special case
    _other_char = ["\n", ",", "➡️", " ", '👉']
    for _o in _other_char:
        if _o in _split_text:
            _split_again = _split_text.split(_o)
            _may_be = None
            if len(_split_again) > 0:
                for _s in _split_again:
                    if "gmail.com" in _s.lower() or "mail.com" in _s.lower():
                        _split_text = _s
                        _split_text = _split_text.strip()
                        break

    _split_text = _split_text.lower()

    # some email endup as
    # t.co/ehxo7kjpgk.reg@gmail.com
    # //t.co/ehxo7kjhqm.reg@gmail.com

    if "//t.co/" in _split_text:  # scammer put
        return None
    if "." not in _split_text:
        return None

    if ".com" in _split_text:
        _email = _split_text.split(".com")[0]
        _split_text = "{}{}".format(_email, ".com")
        _split_text = _split_text.strip()

    if "www." in _split_text:
        _split_text = _split_text.replace("www.", "")

    if '@' not in _split_text:
        return None

    if "at" in _split_text:
        _split_text = _split_text.replace("at", "")
        _split_text = _split_text.strip()

    if _split_text.startswith("."):
        _split_text = _split_text[1:len(_split_text)]

    return _split_text


def get_hash_from_image(img_path):
    try:
        with open(img_path, "rb") as f:
            _bytes = f.read()
            readable_hash = hashlib.sha256(_bytes).hexdigest()
            return readable_hash
    except Exception as ex:
        print("Exception occurred in converting to image hash {} ".format(ex))
    return None


def _remove_from_str(_str_val):
    try:
        if _str_val is None:
            return ''
        _clean = ["\n", ",", "\r", '"', "'", '"']

        for c in _clean:
            _str_val.replace(c, " ")

            _str_val = _str_val.strip()
        return _str_val
    except:
        pass
    return _str_val


def get_all_user_from_db():
    _all_users = set()
    for val in MongoDBActor(COLLECTIONS.USER_DETAILS).find():
        if "username" in val:
            _all_users.add(val['username'])
    if None in _all_users:
        _all_users.remove(None)
    return _all_users


def get_all_user_from_timelines():
    _db_users = set(MongoDBActor(COLLECTIONS.TWITTER_TIMELINESS).distinct(key="username"))
    if None in _db_users:
        _db_users.remove(None)
    return _db_users


def get_all_telegram_user_from_db():
    _all_users = set()
    for val in MongoDBActor(COLLECTIONS.TELEGRAM_CHANNELS).find():
        if "channel_handle" in val:
            _all_users.add(val['channel_handle'])

    if None in _all_users:
        _all_users.remove(None)
    return _all_users


def get_full_user_detail(username):
    try:
        for val in MongoDBActor(COLLECTIONS.USER_DETAILS).find(key={'username': username}):
            if 'data' in val:
                _data = val['data']
                if len(_data) > 0:
                    _user_nested_info = _data[0]

                    if 'protected' in _user_nested_info:
                        _protected = "Prot:{}".format(_user_nested_info['protected'])
                    else:
                        _protected = "Prot?"

                    if 'created_at' in _user_nested_info:
                        _created_at = _user_nested_info['created_at'].replace("\n", "").replace("\r",
                                                                                                "").replace(",",
                                                                                                            "")
                        _created_at = _remove_from_str(_created_at)
                    else:
                        _created_at = "Crt?"

                    if 'url' in _user_nested_info:
                        _url = _user_nested_info['url']
                    else:
                        _url = "Url?"

                    if 'verified' in _user_nested_info:
                        _verified = "V:{}".format(_user_nested_info['verified'])
                    else:
                        _verified = "Vrf?"

                    if "name" in _user_nested_info:
                        _name = _user_nested_info['name']. \
                            replace("\n", "").replace("\r", "").replace(",", "").replace('"', "").replace("'",
                                                                                                          "")
                        _name = _remove_from_str(_name.strip())
                    else:
                        _name = "Nam?"

                    if 'description' in _user_nested_info:
                        _description = _user_nested_info['description']. \
                            replace("\n", "").replace("\r", "").replace(",", "").replace('"', "").replace("'",
                                                                                                          "")
                        _description = _remove_from_str(_description.strip())
                    else:
                        _description = "Des?"

                    if "username" in _user_nested_info:
                        _username = _user_nested_info['username']
                    else:
                        _username = "Usn?"

                    if 'location' in _user_nested_info:
                        _location = _user_nested_info['location']
                        _location = _remove_from_str(_location). \
                            replace("\n", "").replace("\r", "").replace(",", "").replace('"', "").replace("'",
                                                                                                          "")
                        _location = _remove_from_str(_location)
                    else:
                        _location = "loc?"

                    if 'public_metrics' in _user_nested_info:
                        if 'followers_count' in _user_nested_info['public_metrics']:
                            _followers_count = _user_nested_info['public_metrics']['followers_count']
                        else:
                            _followers_count = "-"

                        if 'following_count' in _user_nested_info['public_metrics']:
                            _following_count = _user_nested_info['public_metrics']['following_count']
                        else:
                            _following_count = "-"

                        if 'tweet_count' in _user_nested_info['public_metrics']:
                            _tweet_count = _user_nested_info['public_metrics']['tweet_count']
                        else:
                            _tweet_count = "-"

                        if 'listed_count' in _user_nested_info['public_metrics']:
                            _listed_count = _user_nested_info['public_metrics']['listed_count']
                        else:
                            _listed_count = "-"
                    else:
                        _followers_count = "-"
                        _following_count = "-"
                        _tweet_count = "-"
                        _listed_count = "-"

                    _tup = (_username,
                            _name,
                            _verified,
                            _protected,
                            _location,
                            _followers_count,
                            _following_count,
                            _tweet_count,
                            _listed_count,
                            _description,
                            _url,
                            _created_at
                            )
                    return _tup
    except Exception as ex:
        print("Exception occurred! {}".format(ex))

    _tup = ('?', '?', '?', '?', '?', '?', '?', '?', '?', '?', '?', '?')
    return _tup


def get_typosquatted_official_handle():
    _official_dameru_distance = set(MongoDBActor(COLLECTIONS.TXT_SIMILARITY).distinct(key="official_str", filter={
        "damerau_levenshtein.distance": 1,
        "do_exclude": {"$exists": False}
    }))

    if None in _official_dameru_distance:
        _official_dameru_distance.remove(None)
    return list(_official_dameru_distance)


def get_typosquatted_search_handle():
    _search_dameru_distance = set(MongoDBActor(COLLECTIONS.TXT_SIMILARITY).distinct(key="search_str", filter={
        "damerau_levenshtein.distance": 1,
        "do_exclude": {"$exists": False},
        "combo_squatting.is_official_word_present": True
    }))

    if None in _search_dameru_distance:
        _search_dameru_distance.remove(None)
    return list(_search_dameru_distance)


def get_typosquatted_search_handle_of_social_media(social_media):
    _found_accounts = set()
    for val in MongoDBActor(COLLECTIONS.TXT_SIMILARITY).find({
        "damerau_levenshtein.distance": 1,
        "do_exclude": {"$exists": False},
        "combo_squatting.is_official_word_present": True,
        "social_media": social_media
    }):
        if 'search_str' in val:
            _found_accounts.add(val['search_str'])

    if None in _found_accounts:
        _found_accounts.remove(None)
    return list(_found_accounts)


def get_typosquatted_search_handle_from_official_and_social_media(official_str, social_media):
    _search_dameru_distance = set(MongoDBActor(COLLECTIONS.TXT_SIMILARITY).distinct(key="search_str", filter={
        "damerau_levenshtein.distance": 1,
        "do_exclude": {"$exists": False},
        "official_str": official_str,
        "social_media": social_media
    }))

    if None in _search_dameru_distance:
        _search_dameru_distance.remove(None)
    return list(_search_dameru_distance)


def get_combo_squatted_official_handle():
    _official_handle = set(MongoDBActor(COLLECTIONS.COMBO_SQUATTING_SEQUENCE).distinct(key="official_str"))
    if None in _official_handle:
        _official_handle.remove(None)
    return list(_official_handle)


def get_combo_squatted_search_handle_from_official_and_social_media(official_str, social_media):
    _official_handle = set(MongoDBActor(COLLECTIONS.TXT_SIMILARITY).distinct(key="search_str",
                                                                             filter={
                                                                                 'combo_squatting.sequence.2': {
                                                                                     "$exists": True},
                                                                                 'combo_squatting'
                                                                                 '.is_official_word_present': True,
                                                                                 "do_exclude": {"$exists": False},
                                                                                 "official_str": official_str,
                                                                                 "social_media": social_media
                                                                             }
                                                                             ))
    if None in _official_handle:
        _official_handle.remove(None)
    return list(_official_handle)


def get_combo_squatted_search_handle_from_social_media(social_media):
    _found_accounts = set()
    for val in MongoDBActor(COLLECTIONS.TXT_SIMILARITY).find({
        'combo_squatting.sequence.2': {
            "$exists": True},
        'combo_squatting'
        '.is_official_word_present': True,
        "do_exclude": {"$exists": False},
        "social_media": social_media
    }
    ):
        if 'search_str' in val:
            _found_accounts.add(val['search_str'])

    if None in _found_accounts:
        _found_accounts.remove(None)
    return list(_found_accounts)


def get_combo_squatted_search_handle():
    _search_handle = set(MongoDBActor(COLLECTIONS.COMBO_SQUATTING_SEQUENCE).distinct(key="search_str"))
    if None in _search_handle:
        _search_handle.remove(None)
    return list(_search_handle)


def get_fuzzy_squatted_official_handle():
    _filter = {
        'combo_squatting.is_official_word_present': False,
        'damerau_levenshtein.distance': {
            '$lte': 2}, "do_exclude": {"$exists": False}}

    _official_handle = set(MongoDBActor(COLLECTIONS.TXT_SIMILARITY).distinct(key="search_str",
                                                                             filter=_filter
                                                                             ))
    if None in _official_handle:
        _official_handle.remove(None)
    return list(_official_handle)


def get_fuzzy_squatted_search_handle_from_official_and_social_media(official_str, social_media):
    _filter = {'combo_squatting.is_official_word_present': False,
               'damerau_levenshtein.distance': {
                   '$lte': 2}, "do_exclude": {"$exists": False},
               "official_str": official_str,
               "social_media": social_media
               }

    _official_handle = set(MongoDBActor(COLLECTIONS.TXT_SIMILARITY).distinct(key="search_str",
                                                                             filter=_filter
                                                                             ))
    if None in _official_handle:
        _official_handle.remove(None)
    return list(_official_handle)


def get_fuzzy_squatted_search_handle_from_social_media(social_media):
    _filter = {'combo_squatting.is_official_word_present': False,
               'damerau_levenshtein.distance': {
                   '$lte': 2}, "do_exclude": {"$exists": False},
               "social_media": social_media
               }

    _found_accounts = set()
    for val in MongoDBActor(COLLECTIONS.TXT_SIMILARITY).find(_filter):
        if 'search_str' in val:
            _found_accounts.add(val['search_str'])

    if None in _found_accounts:
        _found_accounts.remove(None)
    return list(_found_accounts)


def get_fuzzy_squatted_search_handle():
    _filter = {'combo_squatting.is_official_word_present': False,
               'damerau_levenshtein.distance': {
                   '$lte': 2}, "do_exclude": {"$exists": False}}

    _search_handle = set(MongoDBActor(COLLECTIONS.TXT_SIMILARITY).distinct(key="search_str",
                                                                           filter=_filter
                                                                           ))
    if None in _search_handle:
        _search_handle.remove(None)
    return list(_search_handle)


def is_account_verified(username):
    try:
        for val in MongoDBActor(COLLECTIONS.USER_DETAILS).find(key={'username': username}):
            if 'data' in val:
                _data = val['data']
                if len(_data) > 0:
                    _user_nested_info = _data[0]
                    if 'verified' in _user_nested_info:
                        _verified = _user_nested_info['verified']
                        return bool(_verified)
    except Exception as ex:
        print("Exception occurred! {}".format(ex))

    return None


def is_user_detail_existent(username):
    _data = MongoDBActor(COLLECTIONS.USER_DETAILS).find(key={'username': username})
    for _d in _data:
        if 'username' in _d:
            return True
    return False


def fetch_user_if_not_present(username, do_wait=None, wait=3):
    retry = 1
    while retry < 2:
        is_account_present = is_user_detail_existent(username)
        if is_account_present:
            return True
        if not is_account_present:
            try:
                _user_info = TwitterFeeds(username).fetch_user_detail_by_screen_name()
                _user_info['username'] = username
                if do_wait:
                    time.sleep(wait)
                if _user_info is not None:
                    MongoDBActor(COLLECTIONS.USER_DETAILS).find_and_modify(
                        key={'username': username},
                        data=_user_info)
                    return True
            except Exception as ex:
                MongoDBActor("users_anamoly").insert_data({
                    'username': username,
                    'error': str(ex)
                })
                print("Exception occurred {}".format(ex))
                time.sleep(1)

        retry += retry + 1
    return False


def get_user_id_from_user_detail(username):
    try:
        for val in MongoDBActor(COLLECTIONS.USER_DETAILS).find(key={'username': username}):
            if 'data' in val:
                _data = val['data']
                if len(_data) > 0:
                    _user_nested_info = _data[0]
                    if 'id' in _user_nested_info:
                        _id = _user_nested_info['id']
                        return _id
    except Exception as ex:
        print("Exception occurred! {}".format(ex))

    return None


def get_regex_by_service_name(service_name):
    return "^[a-z0-9](\.?[a-z0-9]){5,}@{}}\.com$".format(service_name.lower())


def generate_str(_regex):
    _str = rstr.xeger(_regex)
    return _str


def get_all_candidate_domains():
    _domains = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="domain", filter={"is_candidate": True}))
    if None in _domains:
        _domains.remove(None)
    return list(_domains)


def get_web_categories_from_domain(domain):
    for val in MongoDBActor(COLLECTIONS.DOMAIN).find({"domain": domain}):
        if 'web_category' in val:
            return val['web_category']
    return '?'


def get_klazify_categories_from_domain(domain):
    _categories = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="klazify_data.domain.categories.name",
                                                                filter={"domain": domain}))

    if None in _categories:
        _categories.remove(None)

    if len(_categories) == 0:
        return '?'

    _include_categories = include_categories()
    for _i in _include_categories:
        for _c in _categories:
            if _i in _c:
                return _c
    return _categories[0]


def get_social_media_info_from_domain(domain):
    _social_media_info = {}
    for val in MongoDBActor(COLLECTIONS.DOMAIN).find({"domain": domain}):
        if 'twitter' in val:
            _social_media_info['twitter'] = val['twitter']

        if 'instagram' in val:
            _social_media_info['instagram'] = val['instagram']

        if 'youtube' in val:
            _social_media_info['youtube'] = val['youtube']

        if 'twitter' in _social_media_info and 'instagram' in _social_media_info and 'youtube' in _social_media_info:
            return _social_media_info

        _all_social_media_data_in_domain = []

        if 'klazify_data' in val:
            klazify_data = val['klazify_data']
            _all_social_media_data_in_domain.append(klazify_data)
        if 'klazify_data_social_media' in val:
            klazify_data_required = val['klazify_data_social_media']
            _all_social_media_data_in_domain.append(klazify_data_required)

        for klazify_social_info in _all_social_media_data_in_domain:
            if 'domain' in klazify_social_info:
                _domain_dic = klazify_social_info['domain']
                if 'social_media' in _domain_dic:
                    _social_media_data = _domain_dic['social_media']
                    if _social_media_data is None:
                        continue
                    if 'twitter' not in _social_media_info:
                        if 'twitter_url' in _social_media_data:
                            twitter_url = _social_media_data['twitter_url']
                            if twitter_url is not None:
                                twitter_url = twitter_url.replace("https://twitter.com/", "")
                                twitter_url = twitter_url.replace("http://twitter.com/", "")
                                twitter_url = twitter_url.replace("https://www.twitter.com/", "")
                                twitter_url = twitter_url.replace("http://www.twitter.com/", "")
                                twitter_url = twitter_url.replace("/", "")
                                _social_media_info['twitter'] = twitter_url
                    if 'instagram' not in _social_media_info:
                        if 'instagram_url' in _social_media_data:
                            instagram_url = _social_media_data['instagram_url']
                            if instagram_url is not None:
                                instagram_url = instagram_url.replace("https://www.instagram.com/", "")
                                instagram_url = instagram_url.replace("https://instagram.com/", "")
                                instagram_url = instagram_url.replace("http://www.instagram.com/", "")
                                instagram_url = instagram_url.replace("http://instagram.com/", "")
                                instagram_url = instagram_url.replace("/", "")
                                _social_media_info['instagram'] = instagram_url
                    if 'youtube' not in _social_media_info:
                        if 'youtube_url' in _social_media_data:
                            youtube_url = _social_media_data['youtube_url']
                            if youtube_url is not None:
                                youtube_url = youtube_url.replace("https://youtube.com/", "")
                                youtube_url = youtube_url.replace("http://youtube.com/", "")
                                youtube_url = youtube_url.replace("https://www.youtube.com/", "")
                                youtube_url = youtube_url.replace("http://www.youtube.com/", "")
                                youtube_url = youtube_url.replace("/", "")
                                _social_media_info['youtube'] = youtube_url

    return _social_media_info


def get_text_from_img(img_path):
    try:
        file_to_store = tmp.NamedTemporaryFile(delete=False)

        process = subprocess.Popen(['tesseract', img_path, file_to_store.name], stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
        process.communicate()

        with open(file_to_store.name + '.txt', 'r') as handle:
            result_msg = handle.read()

        os.remove(file_to_store.name + '.txt')
        os.remove(file_to_store.name)

        return result_msg
    except Exception as ex:
        print("Error in taking image OCR {}".format(ex))

    return ""
