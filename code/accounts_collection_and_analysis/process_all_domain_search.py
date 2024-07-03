import argparse

import db_util
import constants
import time
from twitter_accounts_search import SearchAccounts


class AllDomainSearch:
    def __init__(self, processing_function, priority=None):
        self.processing_function = processing_function
        self.priority = priority

    def _get_domain_names(self):
        if self.priority:
            _found_data = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="name",
                                                                                          filter={"priority": int(
                                                                                              self.priority)}))
        else:
            _found_data = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="name"))
        if None in _found_data:
            _found_data.remove(None)
        return list(_found_data)

    def _process_name_only(self):
        _domain_names = self._get_domain_names()
        for name in _domain_names:
            print("Processing domain name:{}".format(name))
            is_already_searched = False
            if is_already_searched:
                continue
            _search = SearchAccounts(domain=name, search_keyword=name)
            _search.process()
            time.sleep(2)

    def _process_support_keywords(self):
        _domain_names = self._get_domain_names()
        _keywords = ["recover", "hack", "support", "help", "assist", "contact", "team"]
        for name in _domain_names:
            for _keyword in _keywords:
                print("Processing name:{}, keyword:{}".format(name, _keyword))
                is_already_searched = self._is_already_searched_keyword(_keyword)
                if is_already_searched:
                    continue
                _search = SearchAccounts(domain=name, search_keyword="{} {}".format(name, _keyword))
                _search.process()
                time.sleep(2)

    def _is_already_searched_keyword(self, _search_keyword):
        _key_word = set(db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_DAILY_DOMAIN_SEARCH) \
                        .distinct(key="domain",
                                  filter={"search_keyword": _search_keyword}))
        return len(_key_word) > 0

    def _process_timelines(self):
        _search = SearchAccounts(timelines=True)
        _search.process()
        time.sleep(2)

    def process(self):
        if self.processing_function == "process_name_only":
            print("Processing name only ...")
            self._process_name_only()
        elif self.processing_function == "process_support_keywords":
            self._process_support_keywords()
        elif self.processing_function == "timelines":
            self._process_timelines()
        else:
            raise Exception("Unsupported request")


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Process all domain search")
    _arg_parser.add_argument("-p", "--process",
                             action="store",
                             required=True,
                             help="function name")
    _arg_parser.add_argument("-s", "--search_priority",
                             action="store",
                             required=False,
                             help="search priority")

    _arg_value = _arg_parser.parse_args()

    if _arg_value.search_priority:
        domain_search = AllDomainSearch(_arg_value.process, _arg_value.search_priority)
    else:
        domain_search = AllDomainSearch(_arg_value.process)
    domain_search.process()
