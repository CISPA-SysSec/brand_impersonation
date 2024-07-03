import argparse
import json

import nltk
import db_util
import shared_util
import constants
import textdistance
import wordninja

nltk.download('words')
from nltk.corpus import words


# Reference to the implementation logic
# ref: https://github.com/life4/textdistance/tree/d3e1f6d2117ac70f63eb027b1223b8ddf42cd4aa?tab=readme-ov-file
class Squatting:
    def __init__(self, official_str, search_str, social_media):
        self.official_str = official_str.lower()
        self.search_str = search_str.lower()
        self.social_media = social_media

    def count_combo_squatting_categories_sequence(self):
        _mapper = {}
        _social_media = ["twitter", "instagram", "telegram", "youtube"]
        _all_srch_accounts = set()
        for _sm in _social_media:
            for val in db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find({"social_media": _sm}):
                _all_srch_accounts.add(val['search_str'])

        print("Total search account from all social media: {}".format(len(_all_srch_accounts)))

        distinct_seq = set(
            db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).distinct(key="combo_squatting.sequence"))

        print("Total sequence:{}".format(len(distinct_seq)))

        _official_str = set(
            db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).distinct(key="official_str"))

        print("Total official {}".format(len(_official_str)))

        distinct_seq = distinct_seq.difference(_official_str)
        distinct_seq = distinct_seq.difference(_all_srch_accounts)

        _processed = set()
        for _sm in _social_media:
            for _seq in distinct_seq:
                if len(_seq) < 2:
                    print("Excluding combosquatting with less than 2 characters ...")
                    continue
                if _seq in _processed:
                    print("Already processed {} ".format(_seq))
                    continue
                _found_ = list(db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).distinct(
                    filter={
                        "combo_squatting.sequence": {"$in": [_seq]},
                        "combo_squatting.is_official_word_present": True,
                        "do_exclude": {"$exists": False}
                    },
                    key="search_str"))
                if len(_found_) < 1:
                    continue

                _found_offical = list(db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).distinct(
                    filter={
                        "combo_squatting.sequence": {"$in": [_seq]},
                        "combo_squatting.is_official_word_present": True,
                        "do_exclude": {"$exists": False}
                    },
                    key="official_str"))

                db_util.MongoDBActor("combo_squat_sequence_info").insert_data({"sequence": _seq,
                                                                               'len_sequence': len(_seq),
                                                                               'len_search_str': len(_found_),
                                                                               'search_str': list(_found_),
                                                                               'official_str': list(_found_offical),
                                                                               'len_official_str': len(
                                                                                   _found_offical),
                                                                               'is_digit': _seq.isdigit(),
                                                                               'is_alphanum': _seq.isalnum(),
                                                                               'is_identifier': _seq.isidentifier(),
                                                                               'is_space': _seq.isspace(),
                                                                               'is_ascii': _seq.isascii(),
                                                                               'is_alpha': _seq.isalpha(),
                                                                               'is_printable': _seq.isprintable(),
                                                                               'is_word': _seq in words.words()
                                                                               })

                if _seq.isdigit():
                    _seq = 'digit'
                if _seq not in _mapper:
                    _mapper[_seq] = list(set(_found_))
                else:
                    _mapper[_seq] = list(set(_mapper[_seq] + list(_found_)))

                print("Found seq:{}, srch_accnts{}".format(_seq, len(_found_)))

                _processed.add(_seq)

        _all = []
        for k, v in _mapper.items():
            v = len(set(v).difference(_official_str))
            _all.append((k, v))

        _all.sort(key=lambda x: x[1], reverse=True)

        with open("report/attributes/combo_squatted/overall.csv", "w") as f_write:
            f_write.write("Words, Search Accounts \n")
            for val in _all:
                f_write.write("{}, {}\n".format(val[0], val[1]))

    def cluster_combo_squatting_words_found(self):
        all_digit = {}
        all_english_word = {}
        other_word = {}
        _official_str = set(db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).distinct(key="official_str",
                                                                                                filter={
                                                                                                    "combo_squatting.is_official_word_present": True,
                                                                                                    "social_media": "twitter"}))
        _len = len(_official_str)
        if len(_official_str) > 0:  # this is bogues for now\
            _found_combo_squatted = set(db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY)
                                        .distinct(key="combo_squatting.combo_words",
                                                  filter={
                                                      "social_media": "twitter",
                                                      "combo_squatting.is_official_word_present": True
                                                  }))

            _len = len(_found_combo_squatted)
            for cnt, _each_combo_squat in enumerate(_found_combo_squatted):
                print("Processing {}/{} {}".format(cnt, _len, _each_combo_squat))
                if not _each_combo_squat:
                    continue
                _search_str = set(db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY)
                                  .distinct(key="search_str",
                                            filter={"combo_squatting.combo_words": _each_combo_squat,
                                                    "combo_squatting.is_official_word_present": True,
                                                    "social_media": "twitter"
                                                    }))
                _search_str = _search_str.difference(_official_str)
                is_num_combo_squat = _each_combo_squat.isdigit()
                is_english_word = _each_combo_squat in words.words()

                if is_num_combo_squat:
                    if _each_combo_squat not in all_digit:
                        all_digit[_each_combo_squat] = list(_search_str)
                    else:
                        _prev = set(all_digit[_each_combo_squat]).union(_search_str)
                        all_digit[_each_combo_squat] = list(_prev)
                elif is_english_word:
                    if _each_combo_squat not in all_english_word:
                        all_english_word[_each_combo_squat] = list(_search_str)
                    else:
                        _prev = set(all_english_word[_each_combo_squat]).union(_search_str)
                        all_english_word[_each_combo_squat] = list(_prev)
                else:
                    if _each_combo_squat not in other_word:
                        other_word[_each_combo_squat] = list(_search_str)
                    else:
                        _prev = set(other_word[_each_combo_squat]).union(_search_str)
                        other_word[_each_combo_squat] = list(_prev)

        for k, v in all_digit.items():
            if not k:
                continue
            with open(
                    "brand_impersonation_data/squatted_handles/combo_squat/all_digit/{}.json".format(
                        k), "w") as f_write:
                for val in v:
                    f_write.write("{}\n".format(val))

        for k, v in all_english_word.items():
            if not k:
                continue
            with open(
                    "brand_impersonation_data/squatted_handles/combo_squat/all_english_word/{}.json".format(
                        k), "w") as f_write:
                for val in v:
                    f_write.write("{}\n".format(val))

        for k, v in other_word.items():
            if not k:
                continue
            with open(
                    "brand_impersonation_data/squatted_handles/combo_squat/other/{}.json".format(
                        k), "w") as f_write:
                for val in v:
                    f_write.write("{}\n".format(val))

    def is_already_processed(self):
        _found_search_str = set(db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).distinct(key="search_str",
                                                                                                    filter={
                                                                                                        "official_str": self.official_str,
                                                                                                        "search_str": self.search_str,
                                                                                                        "combo_squatting": {
                                                                                                            "$exists": True},
                                                                                                        "damerau_levenshtein": {
                                                                                                            "$exists": True},
                                                                                                        "social_media": self.social_media
                                                                                                    }))
        if None in _found_search_str:
            _found_search_str.remove(None)
        if len(_found_search_str) > 0:
            return True
        return False

    def process(self):
        is_processed = self.is_already_processed()
        if is_processed:
            print("Already processed, escaping {}/{}".format(self.official_str, self.search_str))
            return
        _txt_process = {
            "official_str": self.official_str,
            "search_str": self.search_str,
            "social_media": self.social_media,
            'damerau_levenshtein': self.process_damerau_levenshtein(),
            'combo_squatting': self.process_combo_squatting()
        }
        db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).insert_data(_txt_process)

    def process_combo_squatting(self):
        try:
            _combo_words = wordninja.split(self.search_str)
            # remove the first word to find the longest sequence or possible sequence
            _sequence = wordninja.split(self.search_str)
            if self.official_str in _sequence:
                _sequence.remove(self.official_str)
            _data_ = {
                'is_official_word_present': self.official_str in _combo_words,
                'combo_words': _combo_words,
                'sequence': _sequence
            }
            print(_data_)
            return _data_
        except Exception as ex:
            print("Exception occurred official:{}, search:{}, ex:{}".format(self.official_str, self.search_str, ex))

    def process_damerau_levenshtein(self):
        try:
            distance = textdistance.damerau_levenshtein.distance(self.official_str, self.search_str)
            similarity = textdistance.damerau_levenshtein.similarity(self.official_str, self.search_str)
            normalized_distance = textdistance.damerau_levenshtein.normalized_distance(self.official_str,
                                                                                       self.search_str)
            normalized_similarity = textdistance.damerau_levenshtein.normalized_similarity(self.official_str,
                                                                                           self.search_str)
            qval_distance = textdistance.DamerauLevenshtein(qval=2).distance(self.official_str, self.search_str)
            _data_ = {
                'distance': distance,
                'similarity': similarity,
                'normalized_distance': normalized_distance,
                'normalized_similarity': normalized_similarity,
                'qval_distance': qval_distance
            }
            print(_data_)
            return _data_
        except Exception as ex:
            print("Exception occurred official:{}, search:{}, ex:{}".format(self.official_str, self.search_str, ex))


class RequestSocialMediaSquatting:
    def __init__(self, social_media):
        self.social_media = social_media

    def process(self):
        if self.social_media == "twitter":
            self.process_twitter()
        elif self.social_media == "instagram":
            self.process_instagram()
        elif self.social_media == "telegram":
            self.process_telegram()
        elif self.social_media == "youtube":
            self.process_youttube()
        elif self.social_media == "combo_squat_cluster":
            self.process_combo_squat_cluster()
        elif self.social_media == "count_combo_squatting_categories_sequence":
            self.process_count_combo_squatting_categories_sequence()
        else:
            raise Exception("Unsupported request for social media process!")

    def process_count_combo_squatting_categories_sequence(self):
        req = Squatting('a', 'a', 'a')
        req.count_combo_squatting_categories_sequence()

    def process_combo_squat_cluster(self):
        req = Squatting('a', 'a', 'a')
        req.cluster_combo_squatting_words_found()

    def process_twitter(self):
        _official_domains = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="domain", filter={
            "is_twitter_candidate": True
        }))

        _len_ = len(_official_domains)
        for cnt, _off_domain in enumerate(_official_domains):
            if not _off_domain:
                continue
            _domain_name = shared_util.get_name_from_domain(_off_domain)
            _official_handle = shared_util.get_social_media_handle_from_domain(_off_domain, 'twitter')
            _found_usernames = shared_util.get_twitter_found_usernames_from_domain_name(_domain_name)
            _total_user = len(_found_usernames)
            print("Processing {}/{}, {}, {}, total_user:{}".format(cnt, _len_, _off_domain, _domain_name, _total_user))
            if None in _found_usernames:
                _found_usernames.remove(None)
            for _each_user_name in _found_usernames:
                if not _each_user_name:
                    continue
                # ensure the official and search account is escaped
                if _official_handle.lower() == _each_user_name.lower():
                    continue
                _req_squat = Squatting(_official_handle, _each_user_name, 'twitter')
                _req_squat.process()

    def process_instagram(self):
        _official_domains = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="domain", filter={
            "is_instagram_candidate": True
        }))

        _len_ = len(_official_domains)
        for cnt, _off_domain in enumerate(_official_domains):
            if not _off_domain:
                continue
            _domain_name = shared_util.get_name_from_domain(_off_domain)
            _official_handle = shared_util.get_social_media_handle_from_domain(_off_domain, 'instagram')
            _found_usernames = shared_util.get_instagram_found_usernames_from_domain_name(_domain_name)
            _total_user = len(_found_usernames)
            print("Processing {}/{}, {}, {}, total_user:{}".format(cnt, _len_, _off_domain, _domain_name, _total_user))
            if None in _found_usernames:
                _found_usernames.remove(None)
            for _each_user_name in _found_usernames:
                if not _each_user_name:
                    continue
                # ensure the official and search account is escaped
                if _official_handle.lower() == _each_user_name.lower():
                    continue
                _req_squat = Squatting(_official_handle, _each_user_name, 'instagram')
                _req_squat.process()

    def process_telegram(self):
        _official_domains = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="domain", filter={
            "is_candidate": True
        }))

        _len_ = len(_official_domains)
        for cnt, _off_domain in enumerate(_official_domains):
            if not _off_domain:
                continue
            _domain_name = shared_util.get_name_from_domain(_off_domain)
            _official_handle = shared_util.get_social_media_handle_from_domain(_off_domain, 'telegram')
            _found_usernames = shared_util.get_telegram_found_usernames_from_domain_name(_domain_name)
            _total_user = len(_found_usernames)
            print("Processing {}/{}, {}, {}, total_user:{}".format(cnt, _len_, _off_domain, _domain_name, _total_user))
            if None in _found_usernames:
                _found_usernames.remove(None)
            for _each_user_name in _found_usernames:
                if not _each_user_name:
                    continue
                _req_squat = Squatting(_official_handle, _each_user_name, 'telegram')
                _req_squat.process()

    def process_youttube(self):
        _official_domains = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="domain", filter={
            "is_youtube_candidate": True
        }))

        _len_ = len(_official_domains)
        for cnt, _off_domain in enumerate(_official_domains):
            if not _off_domain:
                continue
            _domain_name = shared_util.get_name_from_domain(_off_domain)
            _official_handle = shared_util.get_social_media_handle_from_domain(_off_domain, 'youtube')
            _found_channel_url = shared_util.get_youtube_found_usernames_from_domain_name(_domain_name)
            _found_usernames = set()
            # curate the channel url to create handle
            for _channel_url in _found_channel_url:
                _found_username = shared_util.get_youtube_handle_from_channel_url(_channel_url)
                if _found_username:
                    _found_usernames.add(_found_username)
            _total_user = len(_found_usernames)
            print("Processing {}/{}, {}, {}, total_user:{}".format(cnt, _len_, _off_domain, _domain_name, _total_user))
            if None in _found_usernames:
                _found_usernames.remove(None)
            for _each_user_name in _found_usernames:
                if not _each_user_name:
                    continue
                if _official_handle.lower() == _each_user_name.lower():
                    continue
                _req_squat = Squatting(_official_handle, _each_user_name, 'youtube')
                _req_squat.process()

    def update_candidate_flag_to_search_accounts(self):
        pass

    def get_exclude_candidate_lists(self):
        exclude_lists = set()
        # Case I: Filter all official names
        _officials = ["twitter", "instagram", "youtube", "name"]
        for _official in _officials:
            _official_handle = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key=_official))
            for _each_user in _official_handle:
                if _each_user:
                    _each_user = _each_user.lower()  # ensure lower case
                    exclude_lists = exclude_lists.add(_each_user)
        # Case II: add two lds check against 1 million dataset of domain - Our search results show related accounts
        # that are not in our 10K domain rank, thus we exclude these domains to large domain rank 1 million
        # Telegram are not common among social media, thus exclude the check against social media found account
        _social_media = ["twitter", "instagram", "youtube"]
        for each_social in _social_media:
            _search_handle = set(db_util.MongoDBActor(
                constants.COLLECTIONS.TXT_SIMILARITY).distinct(key="search_str",
                                                               filter={"social_media": each_social}))
            for _each_user in _search_handle:
                if _each_user:
                    _each_user = _each_user.lower()  # ensure lower case
                    is_found = shared_util.is_two_ld_present_in_external_fiterlists_db(_each_user)
                    if is_found:
                        exclude_lists.add(is_found)
        return exclude_lists


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Process squatting for each social media")
    _arg_parser.add_argument("-s", "--function",
                             action="store",
                             required=True,
                             help="Each social media channel squatting compute between official and search text")

    _arg_value = _arg_parser.parse_args()

    _request_ = RequestSocialMediaSquatting(_arg_value.function)
    _request_.process()
