import argparse
import json
import shared_util
from db_util import MongoDBActor
from constants import COLLECTIONS


class GenerateGraphData:
    def __init__(self, function_name):
        self.function_name = function_name

    def process(self):
        if self.function_name == "generate_combo_squatting_sequence_length_data":
            self.generate_combo_squatting_sequence_length_data()
        elif self.function_name == "generate_combo_squatting_type_of_sequence_data":
            self.generate_combo_squatting_type_of_sequence_data()
        elif self.function_name == "squatting_impersonating_brand_by_web_category":
            self.squatting_impersonating_brand_by_web_category()
        elif self.function_name == "all_squatted_brand_and_search_str":
            self.all_squatted_brand_and_search_str()

    def generate_combo_squatting_sequence_length_data(self):
        _data_ = []
        _len_sequence = set(MongoDBActor(COLLECTIONS.COMBO_SQUATTING_SEQUENCE).distinct(key="len_sequence"))
        for each_len in _len_sequence:
            official_ = set(MongoDBActor(COLLECTIONS.COMBO_SQUATTING_SEQUENCE).distinct(key="official_str", filter={
                "len_sequence": each_len
            }))
            search_ = set(
                MongoDBActor(COLLECTIONS.COMBO_SQUATTING_SEQUENCE).distinct(key="search_str", filter={
                    "len_sequence": each_len
                }))

            _data_.append((each_len, len(official_), len(search_)))

        recreated_data = []
        recreated_data.sort(key=lambda x: x[0], reverse=False)
        for cnt, _each_data in enumerate(_data_):
            recreated_data.append(
                {'sequence_length': _each_data[0], 'official_account': _each_data[1], 'search_account': _each_data[2]})

        _data_.sort(key=lambda x: x[0], reverse=False)
        with open("report/graph_data/combo_squatting_sequence_length_data.json", "w") as f_read:
            json.dump(recreated_data, f_read, indent=4)

    def generate_combo_squatting_type_of_sequence_data(self):
        _len_digit_search_str = set(MongoDBActor(COLLECTIONS.COMBO_SQUATTING_SEQUENCE).distinct(key="search_str",
                                                                                                filter={
                                                                                                    "is_digit": True
                                                                                                }))
        _len_alphanum_search_str = set(MongoDBActor(COLLECTIONS.COMBO_SQUATTING_SEQUENCE).distinct(key="search_str",
                                                                                                   filter={
                                                                                                       "is_alphanum": True
                                                                                                   }))
        _len_english_word_search_str = set(MongoDBActor(COLLECTIONS.COMBO_SQUATTING_SEQUENCE).distinct(key="search_str",
                                                                                                       filter={
                                                                                                           "is_word": True,
                                                                                                       }))

        _others = set(MongoDBActor(COLLECTIONS.COMBO_SQUATTING_SEQUENCE).distinct(key="search_str"))

        _data_ = {
            'is_digit': len(_len_digit_search_str),
            'is_alphanum': len(_len_alphanum_search_str),
            'is_english_word': len(_len_english_word_search_str),
            'others': abs(len(_others) - (
                    len(_len_digit_search_str) +
                    len(_len_alphanum_search_str) +
                    len(_len_english_word_search_str)
            ))

        }
        with open("report/graph_data/combo_squatting_type_of_sequence_data.json", "w") as f_read:
            json.dump(_data_, f_read, indent=4)

    def squatting_impersonating_brand_by_web_category(self):
        _official_dameru_distance = set(shared_util.get_typosquatted_official_handle())
        _official_combo = set(shared_util.get_combo_squatted_official_handle())
        _official_fuzzy = set(shared_util.get_fuzzy_squatted_official_handle())

        _all_squatting_official = _official_dameru_distance.union(_official_combo).union(_official_fuzzy)

        _web_categories = set(
            MongoDBActor(COLLECTIONS.DOMAIN).distinct(key="web_category", filter={"is_candidate": True}))

        _data = []
        _all_official = set()
        for _each_ in _web_categories:
            _intersection_on_each_category = set()
            for sm in ["instagram", "twitter", "youtube"]:
                _official_web_category_officials = set(MongoDBActor(COLLECTIONS.DOMAIN).distinct(key=sm, filter={
                    "web_category": _each_,
                    "is_candidate": True
                }))

                found_intersection = _official_web_category_officials.intersection(_all_squatting_official)
                _intersection_on_each_category = _intersection_on_each_category.union(found_intersection)
                _all_official = _all_official.union(_intersection_on_each_category)
            _data.append({'web_category': _each_, 'official_brand': len(_intersection_on_each_category)})
        _data.append({'all': 'all', 'total_brand': len(_all_official)})

        with open("report/graph_data/squatting_impersonating_brand_by_web_category", "w") as f_write:
            json.dump(_data, f_write, indent=4)

    def all_squatted_brand_and_search_str(self):
        typosquat_official = set(shared_util.get_typosquatted_official_handle())
        typosquat_search = set(shared_util.get_typosquatted_search_handle())

        combosquat_official = set(shared_util.get_combo_squatted_official_handle())
        combosquat_search = set(shared_util.get_combo_squatted_search_handle())

        fuzzy_official = set(shared_util.get_fuzzy_squatted_official_handle())
        fuzzy_search = set(shared_util.get_fuzzy_squatted_search_handle())

        _data_ = [{
            'typosquat_brand_account': len(typosquat_official),
            'typosquat_search_account': len(typosquat_search),
            'combosquat_brand_account': len(combosquat_official),
            'combosquat_search_account': len(combosquat_search),
            'fuzzy_brand_account': len(fuzzy_official),
            'fuzzy_search_account': len(fuzzy_search),
            'all_official': len(typosquat_official.union(combosquat_official).union(fuzzy_official)),
            'all_search': len(typosquat_search.union(combosquat_search).union(fuzzy_search))
        }]
        with open("report/graph_data/overall_squatting_data.json", "w") as f_write:
            json.dump(_data_, f_write, indent=4)


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="APIFY API Search")
    _arg_parser.add_argument("-p", "--process_name",
                             action="store",
                             required=True,
                             help="processing function name")

    _arg_value = _arg_parser.parse_args()

    _invoke = GenerateGraphData(_arg_value.process_name)
    _invoke.process()
