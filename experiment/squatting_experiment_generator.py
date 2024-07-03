import argparse
import json

import constants
import db_util
import random
import shared_util

"""
    Generate squatting-based profile for a target domain
    Ex. Amazon.com, generate a social media profile that scammer likely to target "amazon" user handle
    
    The csv file added "hit_users.csv" presents the user that was found based on the generation logic. 
    For more information on experiment setting, please refer to our paper section on Experiment.
    
"""


class GenerateSquattingHandle:
    def __init__(self, handle_name=None):
        self.handle_name = handle_name

    def get_test_brands(self):
        _brands = ["paypal", "netflix", "toyota", "paypal", "microsoft", "binance",
                   "google", "facebook", "amazon", "airbnb"]
        return _brands

    # Twitter profile creation rule: letter, number and underscore, and up to 15 characters
    def typosquat_a_handle(self):
        _candidates = {}
        _get_db_rules = set(db_util.MongoDBActor("squatt_format").distinct(key="twitter"))
        for _brand in self.get_test_brands():
            cnt = 1
            do_continue = True
            _found = set()
            while len(_found) < 50:
                for cnt, val in enumerate(list(_get_db_rules)):
                    if len(_found) == 50:
                        break
                    if cnt % 2 == 0 or cnt % 7:
                        _candidate = val.format(_brand)
                    elif cnt % 5 or cnt % 3:
                        _index = random.randint(1, len(_brand) - 1)
                        _str = list(_brand)
                        _str[_index] = "_"
                        _candidate = "".join(_str)
                    else:
                        _replacer = {
                            "b": "6",
                            "c": "3",
                            "g": "9",
                            "l": "1",
                            "s": "5",
                            "v": "u",
                            "z": "2",
                            "i": "1"
                        }
                        _index = random.randint(0, len(_brand))
                        _char_found = _brand[_index]
                        if _char_found in _replacer:
                            _str = list(_brand)
                            _str[_index] = _replacer[_char_found]
                            _candidate = "".join(_str)
                        else:
                            _candidate = val.format(_brand)

                    _is_not_in_db = len(
                        set(db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).distinct(key="search_str",
                                                                                                filter={
                                                                                                    "search_str": _candidate
                                                                                                }))) < 1

                    # twitter rule to be less than 15
                    if _is_not_in_db and len(_candidate) < 15:
                        print("candidate:{}, brand:{}, total:{}".format(_candidate, _brand, len(_found)))
                        _found.add(_candidate)
            _candidates[_brand] = list(_found)
        db_util.MongoDBActor("experiment_domain").insert_data({'typo_squatted_candidate': _candidates})

    def combosquat_a_handle(self):
        _candidates = {}
        _get_db_rules = set(db_util.MongoDBActor("combo_squatt_format").distinct(key="twitter"))
        for _brand in self.get_test_brands():
            do_continue = True
            _found = set()
            while len(_found) < 50:
                for cnt, val in enumerate(list(_get_db_rules)):
                    if len(_found) == 50:
                        break
                    _candidate = val.format(_brand)
                    _is_not_in_db = len(
                        set(db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).distinct(key="search_str",
                                                                                                filter={
                                                                                                    "search_str": _candidate
                                                                                                }))) < 1
                    # twitter rule to be less than 15
                    if _is_not_in_db and len(_candidate) < 15:
                        print("candidate:{}, brand:{}, total:{}".format(_candidate, _brand, len(_found)))
                        _found.add(_candidate)
            _candidates[_brand] = list(_found)
        db_util.MongoDBActor("experiment_domain").insert_data({'combo_squatted_candidate': _candidates})

    def populate_typo_squatted_handle(self):
        all_candidate = {}
        for _sm in ["twitter", "instagram", "telegram", "youtube"]:
            for val in db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find({
                "social_media": _sm,
                "damerau_levenshtein.distance": 1,
                "do_exclude": {"$exists": False}}):
                if 'official_str' in val and 'search_str' in val:
                    _official_candidate = val['official_str']
                    _search_candidate = val['search_str']
                    if _official_candidate in _search_candidate:
                        _create_squat_handle = _search_candidate.replace(_official_candidate, "{}")
                        if _create_squat_handle is None:
                            continue
                        if _sm not in all_candidate:
                            all_candidate[_sm] = [_create_squat_handle]
                        else:
                            _new = list(set(all_candidate[_sm] + [_create_squat_handle]))
                            all_candidate[_sm] = _new
        db_util.MongoDBActor("squatt_format").insert_data(all_candidate)

    def populate_combo_squatted_handle(self):
        _interested_keyword = [
            "service",
            "vouchers",
            "gift",
            "union",
            "hack",
            "tech",
            "sell",
            "ticket",
            "digit",
            "team",
            "help",
            "me",
            "official",
            "pay",
            "info",
            "my",
            "com",
            "link",
            "trade",
            "fund",
            "tech",
            "hub",
            "chat",
            "buy",
            "cloud",
            "sales",
            "hel",
            "dsk",
            "free",
            "flip",
            "drop",
            "desk",
            "fans"
        ]

        all_candidate = {}

        for _brand in self.get_test_brands():
            for _sm in ["twitter", "instagram", "youtube", "telegram"]:
                for val in db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find({
                    "official_str": _brand,
                    "do_exclude": {"$exists": False},
                    "social_media": _sm,
                    "combo_squatting.is_official_word_present": True}):
                    _official_candidate = val['official_str']
                    _search_candidate = val['search_str']
                    if _official_candidate in _search_candidate:
                        _create_squat_handle = _search_candidate.replace(_official_candidate, "{}")
                        do_continue = False
                        for _k in _interested_keyword:
                            if _k in _create_squat_handle:
                                do_continue = True

                        if not do_continue:
                            continue

                        if _create_squat_handle is None:
                            continue
                        if _sm not in all_candidate:
                            all_candidate[_sm] = [_create_squat_handle]
                        else:
                            _new = list(set(all_candidate[_sm] + [_create_squat_handle]))
                            all_candidate[_sm] = _new

        db_util.MongoDBActor("combo_squatt_format").insert_data(all_candidate)


class SquattingUserHandleQueryTwitter:
    def __init__(self):
        pass

    def get_users_by_brand(self):
        _brands = ["paypal", "netflix", "toyota", "paypal", "microsoft", "binance",
                   "google", "facebook", "amazon", "airbnb"]
        _collections = ["typo_squatted_candidate", "combo_squatted_candidate"]

        all_brand_users = {}
        for _b in _brands:
            brand_users = set()
            for _c in _collections:
                _distinct_users = set(db_util.MongoDBActor("experiment_domain").distinct(key="{}.{}".format(_c, _b)))
                brand_users = brand_users.union(_distinct_users)
            all_brand_users[_b] = list(brand_users)
        return all_brand_users

    def get_users(self):
        _brands = ["paypal", "netflix", "toyota", "paypal", "microsoft", "binance",
                   "google", "facebook", "amazon", "airbnb"]

        _collections = ["typo_squatted_candidate", "combo_squatted_candidate"]

        _all_users = set()
        for _c in _collections:
            for _b in _brands:
                _distinct_users = set(db_util.MongoDBActor("experiment_domain").distinct(key="{}.{}".format(_c, _b)))
                _all_users = _all_users.union(_distinct_users)
        if None in _all_users:
            _all_users.remove(None)
        return list(_all_users)

    def process_users_to_check_if_found(self):
        _all_users = self.get_users()
        print("Total users:{}".format(_all_users))
        print("Users len:{}".format(len(_all_users)))

        _all_found = set()
        for cnt, user_id in enumerate(_all_users):
            print("Processing cnt:{}, user:{}".format(cnt, user_id))
            _is_found = shared_util.fetch_user_if_not_present(user_id)
            if _is_found:
                _all_found.add(user_id)
            print("Processing cnt:{}, user_id:{}, is_found:{}, total_found:{}".format(
                cnt, user_id, _is_found, len(_all_found)))

        db_util.MongoDBActor("experiment_domain").insert_data({'alive_users': list(_all_users)})

    def create_report(self):
        hit_doc = []
        miss_doc = []
        errors = {}
        error_account = set()
        _users_ = self.get_users()
        _verified = set()

        all_hit_users = set()
        all_not_found = set()
        all_suspended = set()
        for _user in _users_:
            for val in db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).find({"username": _user}):
                if 'errors' in val:
                    error_account.add(_user)
                    found_error = val['errors'][0]
                    if 'title' in found_error:
                        _title = found_error['title']
                        miss_doc.append("{}, error:{}".format(_user, _title))
                        if 'Forbidden' in _title:
                            all_suspended.add(_user)
                        if 'Not Found' in _title:
                            all_not_found.add(_user)
                else:
                    all_hit_users.add(_user)
                    _data_ = val['data'][0]
                    is_verified = _data_['verified']
                    hit_doc.append("{}, is_verified:{}".format(_user, is_verified))

        _users_by_brand = self.get_users_by_brand()

        _brands_hit_overall = {}
        _brands_over_all_suspended = {}
        _brands_over_all_not_found = {}
        _quantitative_analysis_document = ["Twitter link, Brand\n"]
        for k, v in _users_by_brand.items():
            hit_users = set(v).intersection(all_hit_users)
            _brands_hit_overall[k] = len(hit_users)
            _brands_over_all_suspended[k] = len(set(_users_by_brand[k]).intersection(all_suspended))
            _brands_over_all_not_found[k] = len(set(_users_by_brand[k]).intersection(all_not_found))
            for hit in hit_users:
                _quantitative_analysis_document.append("https://twitter.com/{}, {}\n".format(hit, k))

        _all = {
            'total_user': len(_users_),
            'hit': hit_doc,
            'hit_count': len(hit_doc),
            'miss': miss_doc,
            'miss_count': len(miss_doc),
            'hit_overall': _brands_hit_overall,
            'suspended_overall': _brands_over_all_suspended,
            'overall_not_found': _brands_over_all_not_found
        }
        with open("report/attributes/experiment/detail.json", "w") as f_write:
            json.dump(_all, f_write, indent=4)

        with open("report/attributes/experiment/hit_users.json", "w") as f_write:
            for val in _quantitative_analysis_document:
                f_write.write(val)


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Data creator for db")
    _arg_parser.add_argument("-f", "--function",
                             action="store",
                             required=True,
                             help="Use this argument for requesting to create data")

    _arg_value = _arg_parser.parse_args()

    sq = SquattingUserHandleQueryTwitter()
    sq.create_report()
