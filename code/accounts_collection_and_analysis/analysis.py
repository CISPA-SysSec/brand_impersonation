import argparse
import collections
import json
import os

import nltk, string
# nltk.download('punkt')

import shared_util

from constants import COLLECTIONS, SIMILARITY_CLASS, ANALYSIS, DATA_PATH
from db_util import MongoDBActor

from sklearn.feature_extraction.text import TfidfVectorizer


class Analysis:
    def __init__(self, analyze_data):
        self.analyze_data = analyze_data
        self.stemmer = nltk.stem.porter.PorterStemmer()
        self.remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)

    def process(self):
        if self.analyze_data == ANALYSIS.COSINE_SIMILARITY:
            return self._process_cosine_similarity()
        elif self.analyze_data == ANALYSIS.PAYPAL_ACCOUNT:
            return self._process_pay_pal_found_accounts()
        elif self.analyze_data == ANALYSIS.CANDIDATE_DOMAIN:
            return self._process_candidate_domain_categories()
        elif self.analyze_data == ANALYSIS.DOMAIN_CATEGORIES:
            return self._process_domain_classify_by_categories()
        elif self.analyze_data == ANALYSIS.DOMAIN_CATEGORIES_COLLATED:
            return self._process_domain_classify_by_categories(collate_categories=True)
        elif self.analyze_data == ANALYSIS.DOMAIN_SOCIAL_MEDIA_HANDLE:
            return self._process_social_media_found_handle_information()
        elif self.analyze_data == ANALYSIS.TLDS:
            return self._process_tlds_domain_data_fetch()
        elif self.analyze_data == ANALYSIS.DOMAIN_STATUS_CODE:
            return self._process_domain_status_code()
        elif self.analyze_data == ANALYSIS.DATA_COLLECT_TWITTER_HANDLE:
            self.process_handles_clustering_data(channel='twitter')
        elif self.analyze_data == ANALYSIS.DATA_COLLECT_TWITTER_TIMELINES:
            self.process_text_clustering_data(channel='twitter')
        elif self.analyze_data == ANALYSIS.DATA_COLLECT_TWITTER_UNWIND_URLS:
            self.process_url_collect(channel='twitter')
        elif self.analyze_data == ANALYSIS.DATA_COLLECT_YOU_TUBE_URLS:
            self.process_you_tube_url_collect(channel='twitter')
        elif self.analyze_data == ANALYSIS.TABLE_OVERALL_DATASET:
            self.process_create_overall_data_table_of_potential_candidates()
        else:
            raise Exception("Unsupported request for function call")

    def process_create_overall_data_table_of_potential_candidates(self):
        _all_table_data = {}
        _all_column_data = {}
        _social_media_collections = {
            'twitter': COLLECTIONS.TWITTER_DAILY_DOMAIN_SEARCH,
            'instagram': COLLECTIONS.INSTAGRAM_ACCOUNT_SEARCH,
            'telegram': COLLECTIONS.TELEGRAM_CHANNELS,
            'youtube': COLLECTIONS.YOUTUBE_VIDEO_SEARCH
        }

        _distinct_categories = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="web_category", filter={
            "is_candidate": True}))
        _twitter_candidates = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="name", filter={
            "is_twitter_candidate": True}))
        _instagram_candidates = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="name", filter={
            "is_instagram_candidate": True}))
        _telegram_candidates = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="name", filter={
            "is_candidate": True}))
        _you_tube_candidates = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="name", filter={
            "is_youtube_candidate": True}))

        for _category in _distinct_categories:
            print(_category)
            _social_medias = list(_social_media_collections.keys())
            _category_data = {}
            for _social_media in _social_medias:
                if _social_media == "telegram":
                    _candidate_flag = {"is_candidate": True, "web_category": _category}
                else:
                    _candidate_flag = {"is_{}_candidate".format(_social_media): True, "web_category": _category}
                _domain_names = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="name", filter=_candidate_flag))

                _all_found_accounts_from_social_media = set()
                for _domain_name in _domain_names:
                    if _social_media == "twitter":
                        _found_accounts = shared_util.get_twitter_found_usernames_from_domain_name(_domain_name)
                    elif _social_media == "instagram":
                        _found_accounts = shared_util.get_instagram_found_ownerID_from_domain_name(_domain_name)
                    elif _social_media == "telegram":
                        _found_accounts = shared_util.get_telegram_found_usernames_from_domain_name(_domain_name)
                    elif _social_media == "youtube":
                        _found_accounts = shared_util.get_youtube_found_usernames_from_domain_name(_domain_name)
                    else:
                        raise Exception("Unsupported type!")
                    _all_found_accounts_from_social_media = _all_found_accounts_from_social_media.union(_found_accounts)

                _category_data[_social_media] = len(_all_found_accounts_from_social_media)
                if _category not in _all_column_data:
                    _all_column_data[_category] = {
                        _social_media: list(_all_found_accounts_from_social_media)
                    }
                else:
                    if _social_media not in _all_column_data[_category]:
                        _all_column_data[_category][_social_media] = list(_all_found_accounts_from_social_media)
                    else:
                        _all_column_data[_category][_social_media] = list(set(_all_column_data[_category][_social_media]).union(_all_found_accounts_from_social_media))
            _all_table_data[_category] = _category_data

            _last_row_table_data_all = {}
            for _category, _social_media in _all_column_data.items():
                if _category not in _last_row_table_data_all:
                    _last_row_table_data_all[_category] = _social_media
                else:
                    _last_row_table_data_all[_category] = list(set(_last_row_table_data_all[_category]).union(set(_social_media)))

                if _social_media not in _last_row_table_data_all:
                    _last_row_table_data_all[_social_media] = _social_media
                else:
                    _last_row_table_data_all[_social_media] = list(
                        set(_last_row_table_data_all[_social_media]).union(set(_social_media)))

            _len_last_row_table_data_all = {}
            for key, values in _last_row_table_data_all.items():
                _len_last_row_table_data_all = len(values)

            _all_table_data['all'] = _len_last_row_table_data_all

        return _all_table_data

    def get_you_tube_found_channel_name_from_domain_name(self, domain_name):
        _channel_handle = set(MongoDBActor(COLLECTIONS.YOUTUBE_APIFY_SEARCH)
                              .distinct(key="channelUrl", filter={"domain_name": domain_name}))
        if None in _channel_handle:
            _channel_handle.remove(None)
        return _channel_handle

    def _process_candidate_domain_categories(self):
        _distinct_domain = shared_util.get_all_candidate_domains()
        _all = []
        for _d in _distinct_domain:
            _rank = shared_util.get_rank_from_domain(_d)
            _category = shared_util.get_web_categories_from_domain(_d)
            print("{},{},{}".format(_rank, _d, _category))
            _all.append((_rank, _d, _category))

        _all.sort(key=lambda tup: tup[0])

        _to_return = ["Rank, Domain, Category\n"]
        for val in _all:
            _to_return.append("{},{},{}\n".format(val[0], val[1], val[2]))
        return _to_return

    def process_url_collect(self, channel='twitter'):
        _all_data = self.get_all_domain_user_handles_from_channel(channel=channel)
        _url_data = {}
        _cnt_ = 1
        for _domain_, _users_ in _all_data.items():
            for count, _user in enumerate(_users_):
                _unwound_url_ = shared_util.get_all_times_lines_url_from_user_name(_user)
                if len(_unwound_url_) > 0:
                    _url_data["{}+{}".format(_domain_, _user)] = list(_unwound_url_)
                    print("Processed: Domain:{}/{}, Found users:{}/{}, Found urls:{}".format(_cnt_, _domain_, count,
                                                                                             len(_users_),
                                                                                             len(_unwound_url_)))
            _cnt_ = _cnt_ + 1
        for _domain_user, _url_found in _url_data.items():
            with open("{}{}.csv".format(DATA_PATH.TWITTER_UNWIND_URLS, _domain_user), "w") as f_write:
                for _url_f in _url_found:
                    f_write.write("{}\n".format(_url_f))

    def process_you_tube_url_collect(self, channel='twitter'):
        _domain_users = self.get_all_domain_user_handles_from_channel(channel=channel)
        cont = 1
        for _domain, _users in _domain_users.items():
            for _u in _users:
                _candidates_url = shared_util.get_all_twitter_times_lines_url_from_user_name_containing_you_tube_link(
                    _u)
                _fpath = "{}{}/{}.csv".format(DATA_PATH.YOURTUBE_URLS, channel, _u)
                if os.path.exists(_fpath):
                    # print("Processing file exists:{}".format(_fpath))
                    continue
                with open(_fpath, "w") as f_write:
                    for _can_url in _candidates_url:
                        f_write.write("{}\n".format(_can_url))
                        print(_u, _can_url, "\n")

    def get_all_domain_user_handles_from_channel(self, channel='twitter'):
        _all_data = {}
        _distinct_domain_names = shared_util.get_all_domain_name()
        for counter, _name in enumerate(_distinct_domain_names):
            if channel == 'twitter':
                _found_users = shared_util.get_usernames_from_daily_twitter_search(_name)
            else:
                continue
            if len(_found_users) > 0:
                _all_data[_name] = _found_users
        return _all_data

    def process_handles_clustering_data(self, channel='twitter'):
        _all_data = self.get_all_domain_user_handles_from_channel(channel=channel)
        for _domain, user_found in _all_data.items():
            with open("{}{}.csv".format(DATA_PATH.TWITTER_HANDLE, _domain), "w") as f_write:
                for _user in user_found:
                    f_write.write("{}\n".format(_user))

    def process_text_clustering_data(self, channel='twitter'):
        _all_data = self.get_all_domain_user_handles_from_channel(channel=channel)

        _curated_domain = {}
        with open("report/attributes/candidate_domain_lists/all_found_twitter_data.csv", "r") as f_read:
            lines = f_read.readlines()

        for counter, line in enumerate(lines):
            if counter == 0:
                continue
            _splitter = line.split(",")

            _domain = _splitter[1].strip()
            _domain = _domain.split(".")[0]
            if len(_splitter) < 3:
                continue

            _twitter_handle = _splitter[2].strip()
            if _twitter_handle is not None:
                _curated_domain[_domain] = _twitter_handle.lower()

        _key_count = 1
        _all_domain = list(_curated_domain.keys())
        _all_official_handles = list(_curated_domain.values())
        print("All domains from file", _all_domain)
        for _domain, user_found in _all_data.items():
            if _domain not in _all_domain:
                print("Domain {} not in list".format(_domain))
                continue
            for _cnt, _user_ in enumerate(user_found):
                if _user_.lower() in _all_official_handles:
                    print("Escaping official handle {}".format(_user_))
                    continue

                if shared_util.is_account_verified(_user_):
                    print("Escaping verified user {}".format(_user_))
                    continue
                _fpath = "{}{}+{}.csv".format(DATA_PATH.TWITTER_TIMELINES, _domain, _user_)
                if os.path.exists(_fpath):
                    print("Path exists, continuing ... {}".format(_fpath))
                    continue
                _timeslines_text = shared_util.get_all_times_lines_text_from_user_name(_user_)
                if len(_timeslines_text) > 0:
                    with open(_fpath, "w") as f_write:
                        for _text_ in _timeslines_text:
                            f_write.write("{}\n-------- line separator --------\n".format(_text_))
                    print("Processed: Domain:{}/{}, Users:{}/{}, Text:{}, f_path:{}".format(_key_count, _domain, _cnt,
                                                                                            len(user_found),
                                                                                            len(_timeslines_text),
                                                                                            _fpath))
            _key_count = _key_count + 1

    def _process_domain_status_code(self):
        _status_code_data = {}
        _domains = shared_util.get_all_domain_url()
        for count, _domain in enumerate(_domains):
            _code = shared_util.get_domain_status_from_db(_domain)
            if _code not in _status_code_data:
                _status_code_data[_code] = [_domain]
            else:
                _prev = _status_code_data[_code]
                _new = _prev + [_domain]
                _status_code_data[_code] = list(set(_new))
            print("Processed {}/{}, domain:{}, code:{}".format(count, len(_domains), _domain, _code))
        return _status_code_data

    def _process_pay_pal_found_accounts(self):
        _accounts_info = ["PayPal Link, PayPal Account Name, Related Search Tag\n"]
        _pay_pal_accounts = shared_util.get_pay_pal_accounts()
        for acc in _pay_pal_accounts:
            _name = shared_util.get_account_name_from_pay_pal_user_name(acc)
            if not _name:
                _name = 'missing'
            if "," in _name:
                _name = _name.replace(",", "")
            _searched_context = shared_util.get_twitter_domain_search_username_search_info(acc)
            if '?' in _searched_context:
                continue
            _str = "https://www.paypal.com/paypalme/{},{},{}\n".format(acc, _name, _searched_context)
            print(_str)
            _accounts_info.append(_str)
        return _accounts_info

    def _process_cosine_similarity(self):
        _domains = shared_util.get_all_domain_name()
        _all_domain_similarity = {}
        for count, _domain in enumerate(_domains):
            print("Processing: {}/{} domain: {}, ".format(count, len(_domains), _domain))
            _line_items = []
            for val in MongoDBActor(COLLECTIONS.TWITTER_DAILY_DOMAIN_SEARCH).find({"domain": _domain}):
                _search_keyword = val['search_keyword']
                _user_names = val['usernames']
                if len(_user_names) == 0:
                    continue
                for _user_name in _user_names:
                    _user_name = _user_name.lower()
                    _line_item = self._create_similarity_line_item_for_text(_domain, _user_name,
                                                                            SIMILARITY_CLASS.COSINE,
                                                                            _search_keyword)
                    _line_items.append(_line_item)
            if len(_line_items) == 0:
                continue
            sorted_by_similarity = sorted(_line_items, key=lambda tup: tup[1], reverse=True)
            _all_domain_similarity[_domain] = sorted_by_similarity
        return _all_domain_similarity

    def _create_similarity_line_item_for_text(self, domain_name, found_handle, similarity_compare, search_keyword):
        if similarity_compare == SIMILARITY_CLASS.COSINE:
            _similarity_value = self._text_similarity_cosine(domain_name, found_handle)
        else:
            _similarity_value = -1

        _u = shared_util.get_full_user_detail(found_handle)

        _matched_account_name = self._clean_string_found(found_handle)
        _set_val = (found_handle, _similarity_value, _u[1], _u[2], _u[3], _u[4], _u[5], _u[6], _u[7],
                    _u[8], _u[9], _u[10], _u[11], search_keyword)
        return _set_val

    def _clean_string_found(self, _str):
        try:
            if _str is None:
                return 'not found'
            _clean = ["\n", ",", "\r", '"', "'"]

            for c in _clean:
                _str = _str.replace(c, " ")

            _str = _str.strip()
            return _str
        except:
            pass
        return _str

    def stem_tokens(self, tokens):
        return [self.stemmer.stem(item) for item in tokens]

    def normalize(self, text):
        return self.stem_tokens(nltk.word_tokenize(text.lower().translate(self.remove_punctuation_map)))

    def _text_similarity_cosine(self, text1, text2):
        try:
            vectorizer = TfidfVectorizer(tokenizer=self.normalize, stop_words='english')
            tfidf = vectorizer.fit_transform([text1, text2])
            return (tfidf * tfidf.T).A[0, 1]
        except Exception as ex:
            print(str(ex))
        return -1

    # The idea is to collate the sub-categories into few giant category
    def _categories_key_from_found_categories(self, found_category):
        found_category = found_category.lower()
        if 'adult' in found_category:
            return 'adult'
        elif 'arts' in found_category:
            return 'arts&entertainment'
        elif 'autos' in found_category:
            return 'autos&vehicles'
        elif 'beauty' in found_category:
            return 'beauty&fitness'
        elif 'books' in found_category:
            return 'books&literature'
        elif 'business' in found_category:
            return 'business&industrial'
        elif 'computer' in found_category:
            return 'computers&electronics'
        elif 'finance' in found_category:
            return 'finance'
        elif 'food' in found_category:
            return 'food&drink'
        elif 'games' in found_category:
            return 'games'
        elif 'health' in found_category:
            return 'health'
        elif 'hobbies' in found_category:
            return 'hobbies'
        elif 'home' in found_category:
            return 'homes&garden'
        elif 'internet' in found_category:
            return 'internet&telecom'
        elif 'jobs' in found_category:
            return 'jobs&education'
        elif 'law' in found_category:
            return 'law&government'
        elif 'news' in found_category:
            return 'news'
        elif 'online' in found_category:
            return 'onlinecommunities'
        elif 'people' in found_category:
            return 'people&society'
        elif 'pets' in found_category:
            return 'pets&animals'
        elif 'real' in found_category:
            return 'realestate'
        elif 'reference' in found_category:
            return 'reference'
        elif 'science' in found_category:
            return 'science'
        elif 'sensitive' in found_category:
            return 'sensitive'
        elif 'shopping' in found_category:
            return 'shopping'
        elif 'sports' in found_category:
            return 'sports'
        elif 'travel' in found_category:
            return 'travel'
        else:
            raise Exception("Unable to find mapping categories")

    def _process_tlds_domain_data_fetch(self):
        _tlds = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="tld"))
        _data_ = []
        for _tld in _tlds:
            _domain_ = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="domain", filter={"tld": _tld}))
            _data_.append((_tld, len(_domain_)))
        _data_.sort(key=lambda tup: tup[1], reverse=True)
        _to_return = []
        for val in _data_:
            _to_return.append("{},{}\n".format(val[0], val[1]))
        print(_to_return)
        return _to_return

    def _process_social_media_found_handle_information(self):
        twitter_data = []
        instagram_data = []
        for i in range(1, 10001):
            _domain = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="domain", filter={"rank": i}))
            if len(_domain) < 1:
                continue
            _domain = list(_domain)[0]
            print(str(i), _domain)
            _url_status_code = shared_util.get_domain_status_from_db(_domain)
            if _url_status_code not in [200, 201, 202, 203, 204]:
                continue
            _social_media_info = shared_util.get_social_media_info_from_domain(_domain)
            _rank = shared_util.get_rank_from_domain(_domain)
            if 'twitter' in _social_media_info:
                _str_append = "{},{},{}".format(_rank, _domain, _social_media_info['twitter'])
                twitter_data.append(_str_append)
            if 'instagram' in _social_media_info:
                _str_append = "{},{},{}".format(_rank, _domain, _social_media_info['instagram'])
                instagram_data.append(_str_append)

        return {'twitter_handle': twitter_data, 'instagram_handle': instagram_data}

    def _process_domain_classify_by_categories(self, collate_categories=False):
        _categories = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="klazify_data.domain.categories.name"))
        _all_data = {}
        _processed_domain = set()
        for counter, _category in enumerate(_categories):
            _data_append = []
            print(str(counter), "/", str(len(_categories)), _category)
            _domains = MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="domain",
                                                                 filter={
                                                                     "klazify_data.domain.categories.name": _category
                                                                 })
            for _domain in _domains:
                _url_status_code = shared_util.get_domain_status_from_db(_domain)
                if _url_status_code not in [200, 201, 202, 203, 204]:
                    continue
                _social_media_info = shared_util.get_social_media_info_from_domain(_domain)
                _rank = shared_util.get_rank_from_domain(_domain)
                if 'twitter' not in _social_media_info:
                    _social_media_info['twitter'] = '?'
                if 'instagram' not in _social_media_info:
                    _social_media_info['instagram'] = '?'

                _str_append = "{},{},{},{}\n".format(_rank, _domain, _social_media_info['twitter'],
                                                     _social_media_info['instagram'])
                if _domain in _processed_domain:
                    continue
                _data_append.append(_str_append)
                _processed_domain.add(_domain)
            if len(_data_append) < 1:
                continue
            if _category.startswith("/"):
                _category = _category[1:len(_category)]
            if "'" in _category:
                _category = _category.replace("'", "")
            _category = _category.replace("/", "_")
            _category = _category.replace('"', '')

            if collate_categories:
                _category = self._categories_key_from_found_categories(_category)
            _all_data[_category] = _data_append
            print(_data_append)
        return _all_data


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Process analysis of the data collected")
    _arg_parser.add_argument("-a", "--analyze_data",
                             action="store",
                             required=True,
                             help="Data processing name for analysis")

    _arg_value = _arg_parser.parse_args()

    if _arg_value.analyze_data not in [ANALYSIS.COSINE_SIMILARITY,
                                       ANALYSIS.PAYPAL_ACCOUNT,
                                       ANALYSIS.DOMAIN_CATEGORIES,
                                       ANALYSIS.DOMAIN_CATEGORIES_COLLATED,
                                       ANALYSIS.DOMAIN_SOCIAL_MEDIA_HANDLE,
                                       ANALYSIS.DOMAIN_STATUS_CODE,
                                       ANALYSIS.TLDS,
                                       ANALYSIS.DATA_COLLECT_TWITTER_HANDLE,
                                       ANALYSIS.DATA_COLLECT_TWITTER_TIMELINES,
                                       ANALYSIS.DATA_COLLECT_TWITTER_UNWIND_URLS,
                                       ANALYSIS.DATA_COLLECT_YOU_TUBE_URLS,
                                       ANALYSIS.CANDIDATE_DOMAIN,
                                       ANALYSIS.TABLE_OVERALL_DATASET]:
        raise Exception("Unsupported data analysis requested")

    _analyze = Analysis(_arg_value.analyze_data)
    _data_ = _analyze.process()

    dir_path = "report/attributes/{}".format(_arg_value.analyze_data)
    isExist = os.path.exists(dir_path)
    if not isExist:
        os.makedirs(dir_path)

    if _arg_value.analyze_data in [ANALYSIS.COSINE_SIMILARITY]:
        for _domain_name, _all_sim_ in _data_.items():
            _headers = [
                "User handle, Similarity Value, Name, Verified, Protected, Location, Followers #, Following #, Tweet #, Listed #, Description, URL, Created Date, Searched Keyword \n"]
            _rank = shared_util.get_rank_from_domain_name(_domain_name)
            for _sim_ in _all_sim_:
                _headers.append(
                    "{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format(_sim_[0], _sim_[1], _sim_[2], _sim_[3],
                                                                         _sim_[4],
                                                                         _sim_[5], _sim_[6], _sim_[7], _sim_[8],
                                                                         _sim_[9],
                                                                         _sim_[10], _sim_[11], _sim_[12], _sim_[13]))

            _f_name = "{}/{}_{}.csv".format(dir_path, _domain_name, _rank)
            with open(_f_name, "w") as f_write:
                for val in _headers:
                    f_write.write(val)
    elif _arg_value.analyze_data in [ANALYSIS.PAYPAL_ACCOUNT, ANALYSIS.TLDS, ANALYSIS.CANDIDATE_DOMAIN]:
        _f_name = "{}/{}".format(dir_path, "detail.csv")
        with open(_f_name, "w") as f_write:
            for val in _data_:
                f_write.write(val)
    elif _arg_value.analyze_data in [ANALYSIS.DOMAIN_CATEGORIES, ANALYSIS.DOMAIN_CATEGORIES_COLLATED]:
        for category, _data_val in _data_.items():
            _f_name = "{}/{}.csv".format(dir_path, category.replace(" ", "").replace("/", "_"))
            with open(_f_name, "w") as f_write:
                for val in _data_val:
                    f_write.write(val)
    elif _arg_value.analyze_data in [ANALYSIS.DOMAIN_STATUS_CODE, ANALYSIS.DOMAIN_SOCIAL_MEDIA_HANDLE]:
        for status_code, _domains_ in _data_.items():
            _f_name = "{}/{}.csv".format(dir_path, status_code)
            with open(_f_name, "w") as f_write:
                for val in _domains_:
                    f_write.write("{}\n".format(val))
    elif _arg_value.analyze_data in [ANALYSIS.TABLE_OVERALL_DATASET]:
        _f_name = "{}/overall_dataset.json".format(dir_path)
        with open(_f_name, "w") as f_write:
            json.dump(_data_, f_write, indent=4)
