import argparse
import json
import re
from db_util import MongoDBActor
from tld import get_tld, get_fld

import constants
import shared_util
import web3


class DataCreator:
    def __init__(self, function_name):
        self.function_name = function_name

    def process(self):
        # This is the master function "candidate_domain_lists" that is for final output
        # The function other than this are curating the results in an iterative fashion
        if self.function_name == "candidate_domain_lists":
            self.create_candidate_domain_lists()
        elif self.function_name == "set_curated_web_content_category_to_domain":
            self.set_curated_web_content_category_to_domain()
        elif self.function_name == "set_flag_candidate_domain_for_social_media":
            self.set_flag_candidate_domain_for_social_media()
        elif self.function_name == "update_missing_twitter_and_instagram_handle_from_manual_entry":
            self.update_missing_twitter_and_instagram_handle_from_manually_added_file()
        elif self.function_name == "update_missing_you_tube_handle_from_manually_added_file":
            self.update_missing_you_tube_handle_from_manually_added_file()
        elif self.function_name == "update_incorrect_you_tube_handle_from_manually_added_file":
            self.update_incorrect_you_tube_handle_from_manually_added_file()
        elif self.function_name == "update_incorrect_twitter_account_by_manual_inspection":
            self.update_incorrect_twitter_account_by_manual_inpsection_from_all_found_twitter_account()
        elif self.function_name == "update_incorrect_instagram_account_by_manual_inspection":
            self.update_incorrect_instagram_account_by_manual_inpsection_from_all_found_instagram_account()
        elif self.function_name == "you_tube_links_from_twitter_posts_of_candidates":
            self.get_youtube_url_from_candidate_posts()
        elif self.function_name == "you_tube_search_account_from_apify_search":
            self.get_you_tube_search_account_from_apify_search()
        elif self.function_name == "telegram_search_account_from_apify_search":
            self.get_telegram_search_account_from_apify_search()
        elif self.function_name == "telegram_search_account_from_telemetr_search":
            self.get_telegram_search_account_from_telemetr_search()
        elif self.function_name == "instagram_search_account_from_apify_search":
            self.get_instagram_search_account_from_apify_search()
        elif self.function_name == "twitter_handles_from_search":
            self.twitter_handles_from_search()
        elif self.function_name == "instagram_handles_from_search":
            self.instagram_handles_from_search()
        elif self.function_name == "telegram_handle_from_telemetr_search":
            self.get_telegram_handle_from_telemetr_search()
        elif self.function_name == "collect_crypto_addresses_from_timelines":
            self.collect_crypto_addresses_from_timelines()
        elif self.function_name == "update_each_field_in_cryptoaddress_found_address":
            self.update_each_field_in_cryptoaddress_found_address()
        elif self.function_name == "create_typo_squatted_data":
            self.create_typo_squatted_data()
        elif self.function_name == "update_combo_squatting_field_is_official_str_present_in_the_sequence":
            self.update_combo_squatting_field_is_official_str_present_in_the_sequence()
        elif self.function_name == "update_exclude_filter_top1_mil_tranco_data_2ld_from_txt_distance_similarity":
            self.update_exclude_filter_top1_mil_tranco_data_2ld_from_txt_distance_similarity()
        elif self.function_name == "update_verified_accounts_from_search_as_do_exclude":
            self.update_verified_accounts_from_search_as_do_exclude()
        elif self.function_name == "cryptoaddresses_data_share":
            self.cryptoaddresses_data_share()
        elif self.function_name == "youtube_blocked_account_update":
            self.youtube_blocked_account_update()
        elif self.function_name == "languages_found":
            self.languages_found()
        else:
            raise Exception("Unsupported function name for create requests!")

    def languages_found(self):
        _all_data = []
        _regions = {}
        _region_based_lang = {}
        _distinct_langagues = set(MongoDBActor(constants.COLLECTIONS.TELEGRAM_CHANNELS).distinct(key="language"))
        for _dl in _distinct_langagues:
            if not _dl:
                continue
            channel_id = set(MongoDBActor(constants.COLLECTIONS.TELEGRAM_CHANNELS).distinct(key="channel_handle",
                                                                                            filter={"language": _dl}
                                                                                            ))
            _all_data.append((_dl, len(channel_id)))

            if "," in _dl:
                splitter = _dl.split(",")
                _region = splitter[0].strip()
                _langauge = splitter[1].strip()
                if _region not in _regions:
                    _regions[_region] = list(channel_id)
                else:
                    _prev = set(_regions[_region] + list(channel_id))
                    _regions[_region] = list(_prev)
                if len(_langauge) > 0:
                    if _langauge not in _region_based_lang:
                        _region_based_lang[_langauge] = list(channel_id)
                    else:
                        _prev = set(_region_based_lang[_langauge] + list(channel_id))
                        _region_based_lang[_langauge] = list(_prev)
                else:
                    _language = splitter[0].strip()
                    if _langauge not in _region_based_lang:
                        _region_based_lang[_langauge] = list(channel_id)
                    else:
                        _prev = set(_region_based_lang[_langauge] + list(channel_id))
                        _region_based_lang[_langauge] = list(_prev)

        _regions_data = []
        for k, v in _regions.items():
            _regions_data.append((k, len(v)))

        _region_specifc_lang_data = []
        for k, v in _region_based_lang.items():
            _region_specifc_lang_data.append((k, len(v)))

        _all_data.sort(key=lambda x:x[1])
        _regions_data.sort(key=lambda x: x[1])
        _region_specifc_lang_data.sort(key=lambda x: x[1])

        with open("report/attributes/languages/overall.csv", "w") as f_write:
            for val in _all_data:
                f_write.write("{},{}\n".format(val[0], val[1]))

        with open("report/attributes/languages/region.csv", "w") as f_write:
            for val in _regions_data:
                f_write.write("{},{}\n".format(val[0], val[1]))

        with open("report/attributes/languages/lang.csv", "w") as f_write:
            for val in _region_specifc_lang_data:
                f_write.write("{},{}\n".format(val[0], val[1]))

    def youtube_blocked_account_update(self):
        with open("report/attributes/youtube_valid_check/youtube_results.csv", "r") as f_read:
            lines = f_read.readlines()
        for cnt, line in enumerate(lines):
            if cnt == 0:
                continue
            splitter = line.split(",")
            url = splitter[0].strip()
            valid = splitter[1].strip()
            valid = valid.lower()
            if valid == "true":
                valid = True
            else:
                valid = False
            MongoDBActor(constants.COLLECTIONS.YOUTUBE_APIFY_SEARCH).find_and_modify(key={"channelUrl": url},
                                                                                     data={"is_alive": valid})
            print(url, valid)

    def cryptoaddresses_data_share(self):
        _processed = set()
        _type = ["btc", "eth"]
        _data_ = []
        cnt = 1
        for _t in _type:
            for val in MongoDBActor(constants.COLLECTIONS.CRYPTO_ADRESS).find({"type": _t}):

                _address = val['cryptoaddress']

                if _address in _processed:
                    continue
                _coin = val['type']
                _text = val['txt']
                _data_.append({
                    'address': _address,
                    'type': val['type'],
                    'txt': _text,
                    'counter': cnt
                })

                _processed.add(_address)
                cnt = cnt + 1

        with open("report/attributes/cryptoaddress/detail.json", "w") as f_read:
            json.dump(_data_, f_read, indent=4)

    def update_verified_accounts_from_search_as_do_exclude(self):
        _all_verified_accounts = set()
        _found_twitter_verified_accounts = set(MongoDBActor(constants.COLLECTIONS.USER_DETAILS).distinct(key="username",
                                                                                                         filter={
                                                                                                             "data.verified": True}))

        _found_instagram_verified_accounts = set(
            MongoDBActor(constants.COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).distinct(key="username",
                                                                                       filter={
                                                                                           "verified": True}))

        _found_related_profiles_instagram_verified_accounts = set(
            MongoDBActor(constants.COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA).distinct(key="relatedProfiles.username",
                                                                                       filter={
                                                                                           "relatedProfiles.is_verified": True}))

        _all_verified_accounts = _found_twitter_verified_accounts.union(
            _found_related_profiles_instagram_verified_accounts).union(_found_instagram_verified_accounts)

        _processed_lower_case_verified_accounts = set()
        for v in _all_verified_accounts:
            _processed_lower_case_verified_accounts.add(v.lower())

        print("All verified accounts found in our database :{}".format(len(_processed_lower_case_verified_accounts)))

        _social_media = ["twitter", "instagram", "name", "youtube"]
        search_handle = set()
        for _sm in _social_media:
            for val in MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find({"social_media": _sm}):
                search_handle.add(val['search_str'])

        difference_to_exclude = _processed_lower_case_verified_accounts.intersection(search_handle)
        _len = len(difference_to_exclude)

        print("Found total intersected account {}".format(_len))

        for cnt, each_intersection in enumerate(difference_to_exclude):
            print("Processing update key: {}/{}, {}".format(cnt, _len, each_intersection))
            MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find_and_modify(key={"search_str": each_intersection},
                                                                               data={"do_exclude": True,
                                                                                     "search_str": each_intersection
                                                                                     }
                                                                               )

    def update_exclude_filter_top1_mil_tranco_data_2ld_from_txt_distance_similarity(self):
        top_10_mil_two_tlds = set()
        print("Processing top 1o mil domains ..")
        with open("data_generator/tranco_lists_1m.csv", "r") as f_read:
            _lines = f_read.readlines()
        for cnt, line in enumerate(_lines):
            if cnt == 0:
                continue
            _splitter = line.split(",")
            domain = _splitter[1]
            domain = domain.replace('"', "")
            domain = domain.strip()
            print("Processing domain {}, {}".format(cnt, domain))
            try:
                res = get_tld("https://{}".format(domain), as_object=True)
                two_tld = res.domain
                if two_tld:
                    top_10_mil_two_tlds.add(two_tld.lower())
            except:
                pass
        _social_media = ["twitter", "instagram", "name", "youtube"]
        official_handle = set()
        print("Processing social media officials ..")
        for _sm in _social_media:
            _distinct = set(MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key=_sm, filter={"is_candidate": True}))
            for _each in _distinct:
                if _each:
                    official_handle.add(_each.lower())

        _combined_exclude = top_10_mil_two_tlds.union(official_handle)

        print("Found {} to be checking for exclusion.".format(len(_combined_exclude)))

        search_handle = set()
        for _sm in _social_media:
            for val in MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find({"social_media": _sm}):
                search_handle.add(val['search_str'])

        print("Total search account found :{}".format(len(search_handle)))
        difference_to_exclude = search_handle.intersection(_combined_exclude)
        _len = len(difference_to_exclude)
        if None in difference_to_exclude:
            difference_to_exclude.remove(None)
        print("Found total handle intersection {}".format(_len))

        for cnt, each_intersection in enumerate(difference_to_exclude):
            print("Processing update key: {}/{}, {}".format(cnt, _len, each_intersection))
            MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find_and_modify(key={"search_str": each_intersection},
                                                                               data={"do_exclude": True,
                                                                                     "search_str": each_intersection
                                                                                     }
                                                                               )

    def update_combo_squatting_field_is_official_str_present_in_the_sequence(self):
        _social_media = ["twitter", "instagram", "telegram", "youtube"]
        for _sm in _social_media:
            _all_officials = set()
            for val in MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find():
                _all_officials.add(val['official_str'])
            for each_official in _all_officials:
                _found_handles = set(MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).distinct(key="search_str",
                                                                                                 filter={
                                                                                                     "social_media": _sm,
                                                                                                     "official_str": each_official}))
                for each_found_handle in _found_handles:
                    if each_official in each_found_handle:
                        MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find_and_modify(
                            key={"search_str": each_found_handle,
                                 "official_str": each_official,
                                 "social_media": _sm
                                 },
                            data={
                                "combo_squatting.is_official_word_present": True
                            })

    def create_typo_squatted_data(self):
        _path = "report/attributes/typo_squatted"
        _social_media = {'twitter', 'instagram', 'youtube', 'telegram'}
        _all_sm = {}

        for _sm in _social_media:
            _tuple_ = []
            _mapper = {}
            search_handles = set(MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).distinct(
                key="search_str",
                filter={"social_media": _sm, "damerau_levenshtein.distance": {"$lte": 2}}
            ))
            brand_handles = set(MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).distinct(
                key="official_str",
                filter={"social_media": _sm, "damerau_levenshtein.distance": {"$lte": 2}}
            ))

            for _each_official in brand_handles:
                each_search_handles = set(MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).distinct(
                    key="search_str",
                    filter={"social_media": _sm, "damerau_levenshtein.distance": {"$lte": 2},
                            "official_str": _each_official}
                ))
                if None in each_search_handles:
                    each_search_handles.remove(None)
                _tuple_.append((_each_official, len(each_search_handles)))
                _mapper[_each_official] = list(each_search_handles)

                if _sm == "telegram":
                    domain_name = shared_util.get_domain_name_from_social_media_handle("name", _each_official)
                else:
                    domain_name = shared_util.get_domain_name_from_social_media_handle(_sm, _each_official)

                if domain_name not in _all_sm:
                    _all_sm[domain_name] = list(each_search_handles)
                else:
                    _prev = _all_sm[domain_name] + list(each_search_handles)
                    _all_sm[domain_name] = list(set(_prev))

            for offstr, srchstr in _mapper.items():
                single_data = {
                    'len': len(srchstr),
                    'search_str': srchstr
                }
                with open("{}/{}/{}.json".format(_path, _sm, offstr), "w") as f_write:
                    json.dump(single_data, f_write, indent=4)

            with open("{}/overall_{}_typo_squatted.json".format(_path, _sm), "w") as f_write:
                _data = {
                    'found_typo_squatted_search_account': len(search_handles),
                    'targeted_official_brand': len(brand_handles)
                }
                json.dump(_data, f_write, indent=4)

            _tuple_.sort(key=lambda x: x[1], reverse=True)
            with open("{}/brand_count_by_search_account_{}_typo_squatted.json".format(_path, _sm), "w") as f_write:
                for val in _tuple_:
                    f_write.write("{}, {}\n".format(val[0], val[1]))

        with open("{}/overall_all_social_media_typo_squatted.json".format(_path, _sm), "w") as f_write:
            _data = {
                'found_typo_squatted_search_account': len(list(_all_sm.values())),
                'targeted_official_brand': len(list(_all_sm.keys()))
            }
            json.dump(_data, f_write, indent=4)

        _tuple_all = []
        for k, v in _all_sm.items():
            _tuple_all.append((k, len(v)))

        _tuple_all.sort(key=lambda x: x[1], reverse=True)

        _tuple_all.sort(key=lambda x: x[1], reverse=True)
        with open("{}/brand_count_by_search_account_all_social_media_typo_squatted.json".format(_path, _sm),
                  "w") as f_write:
            for val in _tuple_all:
                f_write.write("{}, {}\n".format(val[0], val[1]))

    def update_each_field_in_cryptoaddress_found_address(self):
        _social_media = ["twitter", "instagram", "telegram", "youtube"]
        for _each_sm in _social_media:
            all_found_tex = set()
            for each_txt in MongoDBActor(constants.COLLECTIONS.CRYPTO_ADRESS).find({"social_media": _each_sm}):
                if 'txt' in each_txt:
                    found_txt = each_txt['txt']
                if 'text' in each_txt:
                    found_txt = each_txt['text']
                all_found_tex.add(found_txt)

            _all_len = len(all_found_tex)
            for cnt, _each_txt in enumerate(all_found_tex):
                print("Processing {}/{}, {}".format(cnt, _all_len, _each_sm))
                # print(_each_txt)
                original = _each_txt
                splitter = _each_txt.split(" ")
                for each_split in splitter:
                    each_split = each_split.strip()
                    each_split = each_split.replace("\n", " ")
                    each_split = each_split.replace(",", " ")
                    each_split = each_split.replace(":", " ")
                    each_split = each_split.strip()
                    _len = len(each_split)
                    if 25 < _len < 90:
                        is_eth_address = web3.Web3.is_address(each_split)
                        is_btc_address = self.is_valid_btc_address(each_split)
                        if is_eth_address:
                            MongoDBActor(constants.COLLECTIONS.CRYPTO_ADRESS).find_and_modify(key={"txt": original},
                                                                                              data={
                                                                                                  "cryptoaddress": each_split,
                                                                                                  "type": "eth",
                                                                                                  "txt": original,
                                                                                                  "social_media": _each_sm})
                        elif is_btc_address:
                            MongoDBActor(constants.COLLECTIONS.CRYPTO_ADRESS).find_and_modify(key={"txt": original},
                                                                                              data={
                                                                                                  "cryptoaddress": each_split,
                                                                                                  "type": "btc",
                                                                                                  "txt": original,
                                                                                                  "social_media": _each_sm})

    def is_valid_btc_address(self, _str_):
        regex = "^(bc1|[13])[a-km-zA-HJ-NP-Z1-9]{25,34}$"
        p = re.compile(regex)
        if not _str_:
            return False
        if re.search(p, _str_):
            return True
        else:
            return False

    def collect_crypto_addresses_from_timelines(self):
        twitter_text = set()
        _all_regex_txt = []
        for _add_txt in ["btc", "ltc", "eth", "bitcoin", "litecoin", "ethereum", "BTC", "ETH", "LTC"]:
            _regexes = {"$regex": _add_txt}
            _all_regex_txt.append(_regexes)

        print("Processing cryptotwitter timelines ...")
        for _each_reg in _all_regex_txt:
            for val in MongoDBActor(collection_name="timelines", db_name="cryptotwitter").find({"text": _each_reg}):
                if "text" in val:
                    txt = val['text']
                    if txt:
                        # txt = txt.replace("\n", "")
                        twitter_text.add(txt)

        print("Processing username squatting timelines ...")
        for _each_reg in _all_regex_txt:
            for val in MongoDBActor(collection_name=constants.COLLECTIONS.TWITTER_TIMELINESS).find({"text": _each_reg}):
                if "text" in val:
                    txt = val['text']
                    if txt:
                        # txt = txt.replace("\n", "")
                        twitter_text.add(txt)

        telegram_text = set()
        print("Processing telegram accounts ...")
        for _each_reg in _all_regex_txt:
            for val in MongoDBActor(collection_name=constants.COLLECTIONS.TELEGRAM_CHANNELS).find(
                    {"info_content": _each_reg}):
                if "info_content" in val:
                    txt = val['info_content']
                    if txt:
                        # txt = txt.replace("\n", "")
                        telegram_text.add(txt)

        youtube_text = set()
        print("Processing youtube accounts ...")
        for _each_reg in _all_regex_txt:
            for val in MongoDBActor(collection_name=constants.COLLECTIONS.YOUTUBE_APIFY_SEARCH).find(
                    {"text": _each_reg}):
                if "text" in val:
                    txt = val['text']
                    if txt:
                        # txt = txt.replace("\n", "")
                        youtube_text.add(txt)

        print("Processing instagram accounts ...")
        instagram_text = set()
        _collection = [constants.COLLECTIONS.INSTAGRAM_SINGLE_PROFILE_DATA,
                       constants.COLLECTIONS.INSTAGRAM_ACCOUNT_SEARCH]
        for _col in _collection:
            for val in MongoDBActor(_col).find():
                if 'biography' in val:
                    bio = val['biography']
                    # bio = bio.replace("\n", "")
                    instagram_text.add(bio)
                if 'latestPosts' in val:
                    latest_posts = val['latestPosts']
                    for lp in latest_posts:
                        if 'caption' in lp:
                            caption = lp['caption']
                            # caption = caption.replace("\n", "")
                            instagram_text.add(caption)
                if 'topPosts' in val:
                    top_posts = val['topPosts']
                    for lp in top_posts:
                        if 'caption' in lp:
                            caption = lp['caption']
                            instagram_text.add(caption)
                if 'latestIgtvVideos' in val:
                    latest_posts = val['latestIgtvVideos']
                    for lp in latest_posts:
                        if 'caption' in lp:
                            caption = lp['caption']
                            instagram_text.add(caption)

        _data = {
            'twitter': list(twitter_text),
            'instagram': list(instagram_text),
            'telegram': list(telegram_text),
            'youtube': list(youtube_text)
        }

        _related_text = ["bitcoin", "ethereum", "litecoin", "monero", "btc", "eth", "ltc"]
        for social_media, lst_txt in _data.items():
            print("Processing all text from {}".format(social_media))
            for each_txt in lst_txt:
                original_txt = each_txt
                for rlt in _related_text:
                    if rlt in each_txt:
                        do_break = True
                        MongoDBActor(constants.COLLECTIONS.CRYPTO_ADRESS).insert_data(
                            {"txt": original_txt, 'social_media': social_media})
                        break

    def set_curated_web_content_category_to_domain(self):
        _distinct_domain = shared_util.get_all_candidate_domains()
        _all = []
        for _d in _distinct_domain:
            _rank = shared_util.get_rank_from_domain(_d)
            _category = shared_util.get_klazify_categories_from_domain(_d)
            _category = _category[1:]
            if "/" in _category:
                _category = _category.split("/")[0]
                _category = _category.strip()
            _category = _category.replace(" ", "")

            print("{},{},{}".format(_rank, _d, _category))
            MongoDBActor(constants.COLLECTIONS.DOMAIN).find_and_modify({'rank': _rank}, {
                "web_category": _category
            })

    def set_flag_candidate_domain_for_social_media(self):
        _social_media_files = [
            ('youtube', 'report/attributes/candidate_domain_lists/manually_added_missing_youtube_handle.csv'),
            ('youtube', 'report/attributes/candidate_domain_lists/manually_curated_found_you_tube.csv'),
            ('twitter', 'report/attributes/candidate_domain_lists/all_found_twitter_data.csv'),
            ('instagram', 'report/attributes/candidate_domain_lists/all_found_instagram_data.csv')
        ]
        for _sm in _social_media_files:
            _social_media = _sm[0]
            _file = _sm[1]
            with open(_file, "r") as f_read:
                lines = f_read.readlines()
            for counter, line in enumerate(lines):
                print(line)
                if counter == 0:
                    continue
                _splitter = line.split(",")
                if len(_splitter) < 3:
                    continue
                _rank = int(_splitter[0].strip())
                _domain = _splitter[1].strip()
                _handle = _splitter[2].strip()

                print("Processing: count:{},rank:{},domain:{},{} handle:{}".format(counter,
                                                                                   _rank, _domain, _social_media,
                                                                                   _handle
                                                                                   ))

                if _handle != "?":
                    MongoDBActor(constants.COLLECTIONS.DOMAIN).find_and_modify({'rank': _rank}, {
                        "is_candidate": True
                    })

                    MongoDBActor(constants.COLLECTIONS.DOMAIN).find_and_modify({'rank': _rank}, {
                        "is_{}_candidate".format(_social_media): True
                    })

    def get_yout_tube_link_meta_data(self):
        _links = []
        for _link in _links:
            pass

    def get_telegram_handle_from_telemetr_search(self):
        _dir = "brand_impersonation_data/handles/telegram/"
        _channel_handles = set(MongoDBActor(constants.COLLECTIONS.TELEGRAM_CHANNELS).distinct(key="search_text"))
        _curated_domains = shared_util.get_candidate_curated_domains()

        _all_names = set()
        for _c in _curated_domains:
            _name = shared_util.get_name_from_domain(_c)
            _all_names.add(_name)

        for counter, _name in enumerate(_all_names):
            print("Processing Telegram Searched Accounts:{}, {}".format(counter, _name))
            _found_channels = set()
            for val in MongoDBActor(constants.COLLECTIONS.TELEGRAM_CHANNELS).find({'search_text': _name}):
                if 'channel_handle' in val:
                    _found_channels.add(val['channel_handle'])
            if len(_found_channels) < 1:
                continue
            try:
                _f_name = "{}/{}.txt".format(_dir, _name)
                print("Created Filename:{}".format(_f_name))
                with open(_f_name, "w") as f_write:
                    for val in _found_channels:
                        f_write.write("{}\n".format(val))
            except Exception as ex:
                print("Exception occurred {}".format(ex))

    def instagram_handles_from_search(self):
        _instagram_path = "report/attributes/candidate_domain_lists/all_found_instagram_data.csv"
        _save_path_instagram = "brand_impersonation_data/handles/instagram"

        _distinct_search_keywords = set(MongoDBActor(constants.COLLECTIONS.INSTAGRAM_ACCOUNT_SEARCH)
                                        .distinct(key="domain_name"))

        for _sk in _distinct_search_keywords:
            for val in MongoDBActor(constants.COLLECTIONS.INSTAGRAM_ACCOUNT_SEARCH).find({"domain_name": _sk}):
                print(val.keys())
                exit(-1)



        _instagram_write_data = {}
        for _d, _acc in enumerate(_instagram_domain):
            _name = shared_util.get_name_from_domain(_acc)
            print(_d, _acc, _name)
            if not _name:
                continue
            _users = shared_util.get_usernames_from_daily_twitter_search(_name)
            print(_users)
            if _name not in _instagram_write_data:
                _instagram_write_data[_name] = _users
            else:
                _new = list(set(_instagram_write_data[_name] + _users))
                _instagram_write_data[_name] = _new
        for _k, _users in _instagram_write_data.items():
            _f = "{}/{}.txt".format(_save_path_instagram, _k)
            with open(_f, "w") as f_write:
                for val in _users:
                    f_write.write("{}\n".format(val))

    def twitter_handles_from_search(self):
        _instagram_path = "report/attributes/candidate_domain_lists/all_found_instagram_data.csv"
        _twitter_path = "report/attributes/candidate_domain_lists/all_found_twitter_data.csv"
        _you_tube_path = "report/attributes/candidate_domain_lists/all_found_youtube_data.csv"

        _save_path_twitter = "brand_impersonation_data/handles/twitter"
        _save_path_instagram = "brand_impersonation_data/handles/instagram"
        _save_path_telegram = "brand_impersonation_data/handles/telegram"
        _save_path_you_tube = "brand_impersonation_data/handles/youtube"

        _instagram_data = self._get_domain_handle_from_line(_instagram_path)
        _twitter_data = self._get_domain_handle_from_line(_twitter_path)
        _you_tube_data = self._get_domain_handle_from_line(_you_tube_path)

        _instagram_domain = list(_instagram_data.keys())
        _twitter_domain = list(_twitter_data.keys())
        _you_tube_domain = list(_you_tube_data.keys())

        _twitter_write_data = {}
        for _d, _acc in enumerate(_twitter_domain):
            _name = shared_util.get_name_from_domain(_acc)
            print(_d, _acc, _name)
            if not _name:
                continue
            _users = shared_util.get_usernames_from_daily_twitter_search(_name)
            print(_users)
            if _name not in _twitter_write_data:
                _twitter_write_data[_name] = _users
            else:
                _new = list(set(_twitter_write_data[_name] + _users))
                _twitter_write_data[_name] = _new
        for _k, _users in _twitter_write_data.items():
            _f = "{}/{}.txt".format(_save_path_twitter, _k)
            with open(_f, "w") as f_write:
                for val in _users:
                    f_write.write("{}\n".format(val))

    def _get_domain_handle_from_line(self, f_path):
        _data = {}
        with open(f_path, "r") as f_read:
            lines = f_read.readlines()

        for counter, line in enumerate(lines):
            if counter == 0:
                continue
            splitter = line.split(",")
            _domain = splitter[1].strip()
            _handle = splitter[2].strip()
            _data[_domain] = _handle
        return _data

    def get_telegram_search_account_from_telemetr_search(self):
        _dir = "brand_impersonation_data/telemtr/telegram/"
        _channel_handles = set(MongoDBActor(constants.COLLECTIONS.TELEGRAM_CHANNELS).distinct(key="channel_handle"))
        if None in _channel_handles:
            _channel_handles.remove(None)
        if "" in _channel_handles:
            _channel_handles.remove("")
        _found_len = len(_channel_handles)
        for counter, _found_channel in enumerate(_channel_handles):
            print("Processing Telegram Searched Accounts:{}/{}, channel:{}".format(counter, _found_len, _found_channel))
            _data_ = {'channel_handle': _found_channel}
            for val in MongoDBActor(constants.COLLECTIONS.TELEGRAM_CHANNELS).find({'channel_handle': _found_channel}):
                _found_posts = []
                if 'channel_name' in val:
                    _data_['channel_name'] = val['channel_name']
                else:
                    _data_['channel_name'] = None

                if 'info_content' in val:
                    _data_['info_content'] = val['info_content']
                else:
                    _data_['info_content'] = None

                if 'info_header' in val:
                    _data_['info_header'] = val['info_header']
                else:
                    _data_['info_header'] = None

            try:
                _f_name = "{}/{}.json".format(_dir, _found_channel)
                print("Created Filename:{}, Data:{}".format(_f_name, _data_))
                with open(_f_name, "w") as f_write:
                    json.dump(_data_, f_write, indent=4)
            except Exception as ex:
                print("Exception occurred {}".format(ex))

    def get_instagram_search_account_from_apify_search(self):
        _dir = "brand_impersonation_data/apify/instagram/"
        _channel_id = set(MongoDBActor(constants.COLLECTIONS.INSTAGRAM_ACCOUNT_SEARCH).distinct(key="id"))
        if None in _channel_id:
            _channel_id.remove(None)
        if "" in _channel_id:
            _channel_id.remove("")
        _found_len = len(_channel_id)
        for counter, _found_channel in enumerate(_channel_id):
            print(
                "Processing Instagram Searched Accounts:{}/{}, channel:{}".format(counter, _found_len, _found_channel))
            _data_ = {'id': _found_channel}
            _posts = []
            for val in MongoDBActor(constants.COLLECTIONS.INSTAGRAM_ACCOUNT_SEARCH).find({'id': _found_channel}):
                _found_posts = []
                if 'name' in val:
                    _data_['name'] = val['name']
                else:
                    _data_['name'] = None

                if "topPosts" in val:
                    top_posts = val["topPosts"]
                    _found_posts = _found_posts + top_posts

                if "latestPosts" in val:
                    latest_posts = val["latestPosts"]
                    _found_posts = _found_posts + latest_posts

                for _fp in _found_posts:
                    if 'caption' in _fp:
                        _caption = _fp['caption']
                        if _caption not in _posts:
                            _posts.append(_caption)

            _f_name = "{}/{}.json".format(_dir, _found_channel)
            _data_['posts'] = _posts
            print("Created Filename:{}, Data:{}".format(_f_name, _data_))
            with open(_f_name, "w") as f_write:
                json.dump(_data_, f_write, indent=4)

    def get_telegram_search_account_from_apify_search(self):
        _dir = "brand_impersonation_data/apify/telegram/"
        _channel_name = set(MongoDBActor(constants.COLLECTIONS.TELEGRAM_APIFY_SEARCH).distinct(key="channelName"))
        if None in _channel_name:
            _channel_name.remove(None)
        if "" in _channel_name:
            _channel_name.remove("")
        _found_len = len(_channel_name)
        for counter, _found_channel in enumerate(_channel_name):
            print("Processing Telegram Searched Accounts:{}/{}, channel:{}".format(counter, _found_len, _found_channel))
            _data_ = {'channelName': _found_channel}
            for val in MongoDBActor(constants.COLLECTIONS.TELEGRAM_APIFY_SEARCH).find({'channelName': _found_channel}):
                if 'authorName' in val:
                    _data_['authorName'] = val['authorName']
                else:
                    _data_['authorName'] = None

                if 'text' in val:
                    _data_['text'] = val['text']
                else:
                    _data_['text'] = None

                if 'linkPreview' in val:
                    _data_['linkPreview'] = val['linkPreview']
                else:
                    _data_['linkPreview'] = None

            try:
                _f_name = "{}/{}.json".format(_dir, _found_channel)
                print("Created Filename:{}, Data:{}".format(_f_name, _data_))
                with open(_f_name, "w") as f_write:
                    json.dump(_data_, f_write, indent=4)
            except Exception as ex:
                print("oops exception occurred {}".format(ex))

    def get_you_tube_search_account_from_apify_search(self):
        _dir = "brand_impersonation_data/apify/youtube"
        _channel_id = set(MongoDBActor(constants.COLLECTIONS.YOUTUBE_APIFY_SEARCH).distinct(key="id"))
        if None in _channel_id:
            _channel_id.remove(None)
        if "" in _channel_id:
            _channel_id.remove("")
        _found_len = len(_channel_id)
        for counter, _found_channel in enumerate(_channel_id):
            print("Processing YouTube Searched Accounts:{}/{}, channel:{}".format(counter, _found_len, _found_channel))
            _data_ = {'id': _found_channel}
            for val in MongoDBActor(constants.COLLECTIONS.YOUTUBE_APIFY_SEARCH).find({'id': _found_channel}):
                if 'title' in val:
                    _data_['title'] = val['title']
                else:
                    _data_['title'] = None

                if 'text' in val:
                    _data_['text'] = val['text']
                else:
                    _data_['text'] = None

                if 'url' in val:
                    _data_['url'] = val['url']
                else:
                    _data_['url'] = None

                if 'channelName' in val:
                    _data_['channelName'] = val['channelName']
                else:
                    _data_['channelName'] = None

            _f_name = "{}/{}.json".format(_dir, _found_channel)
            print("Created Filename:{}, Data:{}".format(_f_name, _data_))
            with open(_f_name, "w") as f_write:
                json.dump(_data_, f_write, indent=4)

    def get_youtube_url_from_candidate_posts(self):
        _dir = "brand_impersonation_data/posts/twitter"
        you_tube_links = set()
        _lst_size = len(you_tube_links)
        _all_files = ["brand_impersonation_data/you_tube_containing_text.txt"]

        _len_all_files = len(_all_files)
        for counter, _file in enumerate(_all_files):
            print("Processing file {}/{}, {}".format(counter, _len_all_files, _file))
            with open(_file, "r") as f_read:
                lines = f_read.readlines()
            for line in lines:
                _you_tube_link = shared_util.get_you_tube_link_from_line_item(line)
                if _you_tube_link:
                    print("Found:{},{}\n".format(_file, _you_tube_link))
                    you_tube_links.add(_you_tube_link)
                _found_size = len(you_tube_links)
                if len(you_tube_links) % 25 == 0 and _found_size > _lst_size:  # every 25 new find link store in the file for accessing
                    _lst_size = len(you_tube_links)
                    with open(
                            "brand_impersonation_data/youtube_urls/twitter/all_links_by_grep_text.csv",
                            "w") as f_write:
                        for val in you_tube_links:
                            f_write.write("{}\n".format(val))

    # this is curate to all found instagram account file and rerunning the DB with this corrected / curated
    def update_incorrect_instagram_account_by_manual_inpsection_from_all_found_instagram_account(self):
        with open("report/attributes/candidate_domain_lists/manually_curated_file_all_instagram_found.csv",
                  "r") as f_read:
            lines = f_read.readlines()
        for counter, line in enumerate(lines):
            if counter == 0:
                continue
            _splitter = line.split(",")
            if len(_splitter) < 3:
                continue
            _rank = int(_splitter[0].strip())
            _domain = _splitter[1].strip()
            _instagram_handle = _splitter[2].strip()

            print("Processing: count:{},rank:{},domain:{},twitter:{}".format(counter,
                                                                             _rank, _domain,
                                                                             _instagram_handle
                                                                             ))

            MongoDBActor(constants.COLLECTIONS.DOMAIN).find_and_modify({'rank': _rank}, {
                'instagram': _instagram_handle
            })
            print("Updated: {},{},{}".format(_rank, _domain, _instagram_handle))

    # this is curate to all found twitter account file and rerunning the DB with this corrected / curated
    def update_incorrect_twitter_account_by_manual_inpsection_from_all_found_twitter_account(self):
        with open("report/attributes/candidate_domain_lists/manually_curated_file_all_twitter_found.csv",
                  "r") as f_read:
            lines = f_read.readlines()
        for counter, line in enumerate(lines):
            if counter == 0:
                continue
            _splitter = line.split(",")
            if len(_splitter) < 3:
                continue
            _rank = int(_splitter[0].strip())
            _domain = _splitter[1].strip()
            _twitter_handle = _splitter[2].strip()

            print("Processing: count:{},rank:{},domain:{},twitter:{}".format(counter,
                                                                             _rank, _domain,
                                                                             _twitter_handle
                                                                             ))

            MongoDBActor(constants.COLLECTIONS.DOMAIN).find_and_modify({'rank': _rank}, {
                'twitter': _twitter_handle
            })
            print("Updated: {},{},{}".format(_rank, _domain, _twitter_handle))

    # this file is manually searched and updated based on prevalence
    # The process of update was made by manually looking at all found twitter and instagram file
    def update_missing_twitter_and_instagram_handle_from_manually_added_file(self):
        with open("report/attributes/candidate_domain_lists/manually_added_twitter_instagram_handle.csv",
                  "r") as f_read:
            lines = f_read.readlines()
        for counter, line in enumerate(lines):
            if counter == 0:
                continue
            _splitter = line.split(",")
            if len(_splitter) < 4:
                continue
            _rank = int(_splitter[0].strip())
            _domain = _splitter[1].strip()
            _twitter_handle = _splitter[2].strip()
            _instagram_handle = _splitter[3].strip()

            print("Processing: count:{},rank:{},domain:{},twitter:{}, insta:{}".format(counter,
                                                                                       _rank, _domain,
                                                                                       _twitter_handle,
                                                                                       _instagram_handle))

            if _twitter_handle != "?":
                MongoDBActor(constants.COLLECTIONS.DOMAIN).find_and_modify({'rank': _rank}, {
                    'twitter': _twitter_handle
                })
                print("Updated: {},{},{}".format(_rank, _domain, _twitter_handle))

            if _instagram_handle != "?":
                MongoDBActor(constants.COLLECTIONS.DOMAIN).find_and_modify({'rank': _rank}, {
                    'instagram': _instagram_handle
                })
                print("Updated: {},{},{}".format(_rank, _domain, _instagram_handle))

    def update_incorrect_you_tube_handle_from_manually_added_file(self):
        with open("report/attributes/candidate_domain_lists/manually_curated_found_you_tube.csv",
                  "r") as f_read:
            lines = f_read.readlines()
        for counter, line in enumerate(lines):
            print(line)
            if counter == 0:
                continue
            _splitter = line.split(",")
            if len(_splitter) < 3:
                continue
            _rank = int(_splitter[0].strip())
            _domain = _splitter[1].strip()
            _you_tube_handle = _splitter[2].strip()

            print("Processing: count:{},rank:{},domain:{},youtube:{}".format(counter,
                                                                             _rank, _domain,
                                                                             _you_tube_handle
                                                                             ))

            if _you_tube_handle != "?":
                MongoDBActor(constants.COLLECTIONS.DOMAIN).find_and_modify({'rank': _rank}, {
                    'youtube': _you_tube_handle
                })
                print("Updated: {},{},{}".format(_rank, _domain, _you_tube_handle))

    def update_missing_you_tube_handle_from_manually_added_file(self):
        with open("report/attributes/candidate_domain_lists/manually_added_missing_youtube_handle.csv",
                  "r") as f_read:
            lines = f_read.readlines()
        for counter, line in enumerate(lines):
            print(line)
            if counter == 0:
                continue
            _splitter = line.split(",")
            if len(_splitter) < 3:
                continue
            _rank = int(_splitter[0].strip())
            _domain = _splitter[1].strip()
            _you_tube_handle = _splitter[2].strip()

            print("Processing: count:{},rank:{},domain:{},youtube:{}".format(counter,
                                                                             _rank, _domain,
                                                                             _you_tube_handle
                                                                             ))

            if _you_tube_handle != "?":
                MongoDBActor(constants.COLLECTIONS.DOMAIN).find_and_modify({'rank': _rank}, {
                    'youtube': _you_tube_handle
                })
                print("Updated: {},{},{}".format(_rank, _domain, _you_tube_handle))

    def create_candidate_domain_lists(self):
        _all_data = []
        _all_instagram = []
        _all_twitter = []
        _all_youtube = []

        _twitter_missing_data = []
        _instagram_missing_data = []
        _youtube_missing_data = []

        _all_missing_data_that_are_in_category_and_has_accepted_url_status = []

        _domains = list(MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="domain"))
        print(len(_domains))

        _exclude_tlds = [".edu", ".gov"]  # This was necessary as some of the domains were miscategorized from API to
        # the category we are not interested, thus was necessary to check these TLDs

        _twitter_naming_filter = ["University"]  # This was 1 weird case where domain
        # kth.se,KTHuniversity has the twitter handle

        for counter, _domain in enumerate(_domains):
            if _domain is None:
                continue
            do_continue = False
            for _ex_tld in _exclude_tlds:
                if _ex_tld in _domain:
                    do_continue = True
            if do_continue:
                continue
            _is_in_content_category = shared_util.is_domain_in_include_web_content_category(_domain)
            has_accepted_url_status_code = shared_util.is_domain_in_include_status_code(_domain)
            if _is_in_content_category and has_accepted_url_status_code:
                _social_media_info = shared_util.get_social_media_info_from_domain(_domain)
                if 'twitter' in _social_media_info:
                    _twitter = _social_media_info['twitter']
                    if _twitter is not None:
                        for _ex_twitter in _twitter_naming_filter:
                            if _ex_twitter in _twitter:
                                _twitter = '?'
                else:
                    _twitter = '?'
                if 'instagram' in _social_media_info:
                    _instagram = _social_media_info['instagram']
                else:
                    _instagram = '?'

                if 'youtube' in _social_media_info:
                    _youtube = _social_media_info['youtube']
                else:
                    _youtube = '?'
                _rank = shared_util.get_rank_from_domain(_domain)
                if _rank == -1:
                    _rank = 99999999

                if _rank == '?':
                    _rank = 99999999

                if _rank is None:
                    continue
                _str = (_rank, _domain, _twitter, _instagram)
                print(_str)

                if _instagram != "?":
                    _all_instagram.append((_rank, _domain, _instagram))
                if _twitter != "?":
                    _all_twitter.append((_rank, _domain, _twitter))

                if _youtube != "?":
                    _all_youtube.append((_rank, _domain, _youtube))

                if _youtube == "?":
                    _youtube_missing_data.append((_rank, _domain, _youtube))

                if _instagram == "?" and _twitter == "?":
                    _all_missing_data_that_are_in_category_and_has_accepted_url_status.append(_str)
                elif _instagram == "?":
                    _instagram_missing_data.append((_rank, _domain, _instagram))
                elif _twitter == "?":
                    _twitter_missing_data.append((_rank, _domain, _twitter))
                else:
                    _all_data.append(_str)

        _all_data.sort(key=lambda tup: tup[0], reverse=False)
        with open("report/attributes/{}/all_found_data.csv".format(self.function_name), "w") as f_write:
            f_write.write("Rank, Domain, Twitter Handle, Instagram Handle\n")
            for val in _all_data:
                _str = "{},{},{},{}\n".format(val[0], val[1], val[2], val[3])
                f_write.write(_str)

        _all_missing_data_that_are_in_category_and_has_accepted_url_status.sort(key=lambda tup: tup[0], reverse=False)
        with open("report/attributes/{}/missing_both_twitter_and_instagram_data.csv".format(self.function_name),
                  "w") as f_write:
            f_write.write("Rank, Domain, Twitter Handle, Instagram Handle\n")
            for val in _all_missing_data_that_are_in_category_and_has_accepted_url_status:
                _str = "{},{},{},{}\n".format(val[0], val[1], val[2], val[3])
                f_write.write(_str)

        _instagram_missing_data.sort(key=lambda tup: tup[0], reverse=False)
        with open("report/attributes/{}/instagram_missing_data.csv".format(self.function_name), "w") as f_write:
            f_write.write("Rank, Domain, Instagram Handle\n")
            for val in _instagram_missing_data:
                _str = "{},{},{}\n".format(val[0], val[1], val[2])
                f_write.write(_str)

        _twitter_missing_data.sort(key=lambda tup: tup[0], reverse=False)
        with open("report/attributes/{}/twitter_missing_data.csv".format(self.function_name), "w") as f_write:
            f_write.write("Rank, Domain, Twitter Handle\n")
            for val in _twitter_missing_data:
                _str = "{},{},{}\n".format(val[0], val[1], val[2])
                f_write.write(_str)

        _youtube_missing_data.sort(key=lambda tup: tup[0], reverse=False)
        with open("report/attributes/{}/youtube_missing_data.csv".format(self.function_name), "w") as f_write:
            f_write.write("Rank, Domain, YouTube Handle\n")
            for val in _youtube_missing_data:
                _str = "{},{},{}\n".format(val[0], val[1], val[2])
                f_write.write(_str)

        _all_twitter.sort(key=lambda tup: tup[0], reverse=False)
        with open("report/attributes/{}/all_found_twitter_data.csv".format(self.function_name), "w") as f_write:
            f_write.write("Rank, Domain, Twitter Handle\n")
            for val in _all_twitter:
                _str = "{},{},{}\n".format(val[0], val[1], val[2])
                f_write.write(_str)

        _all_instagram.sort(key=lambda tup: tup[0], reverse=False)
        with open("report/attributes/{}/all_found_instagram_data.csv".format(self.function_name), "w") as f_write:
            f_write.write("Rank, Domain, Instagram Handle\n")
            for val in _all_instagram:
                _str = "{},{},{}\n".format(val[0], val[1], val[2])
                f_write.write(_str)

        _all_youtube.sort(key=lambda tup: tup[0], reverse=False)
        with open("report/attributes/{}/all_found_youtube_data.csv".format(self.function_name), "w") as f_write:
            f_write.write("Rank, Domain, YouTube Handle\n")
            for val in _all_youtube:
                _str = "{},{},{}\n".format(val[0], val[1], val[2])
                f_write.write(_str)


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Data creator for db")
    _arg_parser.add_argument("-c", "--create_data",
                             action="store",
                             required=True,
                             help="Use this argument for requesting to create data")

    _arg_value = _arg_parser.parse_args()

    dc = DataCreator(_arg_value.create_data)
    dc.process()
