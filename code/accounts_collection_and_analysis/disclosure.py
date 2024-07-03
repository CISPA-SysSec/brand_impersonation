import argparse

import constants
import shared_util
import db_util
import os


class Disclosure:
    def __init__(self, official_str, social_media):
        self.dir_path = "report/disclosure/{}".format(official_str)
        self.official_str = official_str
        self.social_media = social_media.lower()
        self.exclude_lists = self.all_get_officials()

    def all_get_officials(self):
        if self.social_media in ["twitter", "instagram", "youtube"]:
            _key = self.social_media
        else:
            _key = "name"
        _officials = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key=_key,
                                                                                     filter={"is_candidate": True}))
        lowered_case_officials = set()
        for val in _officials:
            lowered_case_officials.add(val.lower())
        return lowered_case_officials

    def _create_dir_upload_data(self, _f_path, _dir):
        try:
            if not os.path.exists(_f_path):
                os.makedirs(_dir)
        except:
            pass

    def process(self):
        _typo_squatted_handles = set(shared_util.get_typosquatted_search_handle_from_official_and_social_media(
            self.official_str, self.social_media))
        _typo_squatted_handles = _typo_squatted_handles.difference(self.exclude_lists)

        _combo_squatted_handles = set(shared_util.get_combo_squatted_search_handle_from_official_and_social_media(
            self.official_str, self.social_media))
        _combo_squatted_handles = _combo_squatted_handles.difference(self.exclude_lists)
        _combo_squatted_handles = _combo_squatted_handles.difference(_typo_squatted_handles)

        _fuzzy_squatted_handles = set(shared_util.get_fuzzy_squatted_search_handle_from_official_and_social_media(
            self.official_str, self.social_media
        ))

        print("official:{}, typo:{}, combo:{}, fuzzy:{}".format(
            self.official_str,
            len(_typo_squatted_handles),
            len(_combo_squatted_handles),
            len(_fuzzy_squatted_handles)
        ))

        if len(_typo_squatted_handles) > 0:
            d_path = "{}/{}".format(self.dir_path, self.social_media)
            f_path = "{}/typo_squat_data.txt".format(d_path)
            self.write_data(f_path, d_path,_typo_squatted_handles)

        if len(_combo_squatted_handles) > 0:
            d_path = "{}/{}".format(self.dir_path, self.social_media)
            f_path = "{}/combo_squat_data.txt".format(d_path)
            self.write_data(f_path,d_path, _combo_squatted_handles)

        if len(_fuzzy_squatted_handles) > 0:
            d_path = "{}/{}".format(self.dir_path, self.social_media)
            f_path = "{}/fuzzy_squat_data.txt".format(d_path)
            self.write_data(f_path, d_path, _fuzzy_squatted_handles)

    def write_data(self, f_path, d_path, data):
        self._create_dir_upload_data(f_path, d_path)
        with open(f_path, "w") as f_write:
            for val in data:
                if self.social_media == "twitter":
                    _format = "https://twitter.com/{}\n".format(val)
                elif self.social_media == "instagram":
                    _format = "https://instagram.com/{}\n".format(val)
                elif self.social_media == "youtube":
                    _format = "https://youtube.com/{}\n".format(val)
                elif self.social_media == "telegram":
                    _format = "https://t.me/{}\n".format(val)
                else:
                    continue
                f_write.write(_format)


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Process disclosure for social media")
    _arg_parser.add_argument("-s", "--social_media",
                             action="store",
                             required=True,
                             help="Each social media disclosure data")

    _arg_value = _arg_parser.parse_args()

    _official = set()
    for val in db_util.MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find(
            {"social_media": _arg_value.social_media}):
        if 'official_str' in val:
            _official.add(val['official_str'])
    _all_len = len(_official)
    for cnt, each_off in enumerate(_official):
        print("Processing {}, {}".format(cnt, each_off))
        create_disclosure = Disclosure(each_off, _arg_value.social_media)
        create_disclosure.process()
