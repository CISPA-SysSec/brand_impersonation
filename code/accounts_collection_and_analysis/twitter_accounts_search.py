import argparse
import db_util
import constants
import twitter_api
import time
import shared_util
from datetime import datetime

"""
    For provided domain name and searched keyword
        - performs account search
        - stores the user details 
        - stores timeline associated
"""


class SearchAccounts:
    def __init__(self, domain=None, search_keyword=None, timelines=None):
        self.domain = domain
        self.search_keyword = search_keyword
        self.timelines = timelines

    def process(self):
        if self.domain and self.search_keyword and self.timelines:
            print("Processing domain:{}, search keyword:{} and associated timeliness ".format(self.domain,
                                                                                              self.search_keyword))
            self.process_domain_name_search()
            self.process_queued_user_detail_search()
            self.process_timelines_search()
        elif self.domain and self.search_keyword:
            print("Processing domain:{}, search keyword:{}".format(self.domain, self.search_keyword))
            self.process_domain_name_search()
            self.process_queued_user_detail_search()
        elif self.timelines:
            self.process_timelines_search()

    def get_official_twitter_accounts(self):
        _twitter_accounts = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="twitter"))
        _lowered_case = []
        for _accounts in _twitter_accounts:
            _lowered_case.append(_accounts.lower())
        return _lowered_case

    def get_query_time(self):
        return int(time.time() * 1000)

    def get_domain_names(self):
        _domain_names = db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="name")
        if None in _domain_names:
            _domain_names.remove(None)
        return list(_domain_names)

    def process_domain_name_search(self):
        _current_search_time = self.get_query_time()
        _last_search_dates = db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_DAILY_DOMAIN_SEARCH).distinct(
            key="time", filter={"search_keyword": self.search_keyword})
        if len(_last_search_dates) > 0:
            _last_search_dates.sort(reverse=True)
            _last_search = _last_search_dates[0]
            if (_current_search_time - _last_search) < 86400000:
                return
        print("Processing twitter feeds search .. {}".format(self.search_keyword))
        _search_query = twitter_api.TwitterFeeds(search_param=self.search_keyword)
        _search_result = _search_query.fetch_user_name_v1_api()
        _data = {
            'search_keyword': self.search_keyword,
            'domain': self.domain,
            'usernames': _search_result,
            'time': _current_search_time
        }
        _data_insert = db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_DAILY_DOMAIN_SEARCH).insert_data(_data)
        print("Result found, data inserted:{}".format(_data_insert))

        self._queue_searched_username_to_find_user_detail(_current_search_time, _search_result)
        time.sleep(5)  # Add throttle for search

    # Put into queue for future search of data
    def _queue_searched_username_to_find_user_detail(self, search_time, searched_username):
        for account in searched_username:
            is_account_found = shared_util.is_user_detail_existent(account)
            if is_account_found:
                continue

            db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_QUEUE_DAILY_DOMAIN_SEARCH).insert_data({
                'username': account,
                'is_account_detail_processed': False,
                'is_timelines_processed': False,
                'time': search_time,
                'search_keyword': self.search_keyword,
                'domain_name': self.domain
            })

    # Process queued unsearched user detail search
    def process_queued_user_detail_search(self):
        _unsearched_accounts = db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_QUEUE_DAILY_DOMAIN_SEARCH).distinct(
            key="username", filter={'is_account_detail_processed': False, 'search_keyword': self.search_keyword})
        for count, _unsearched in enumerate(_unsearched_accounts):
            if count == 1:
                break
            is_account_found = shared_util.is_user_detail_existent(_unsearched)
            if is_account_found:
                continue
            twitter_api_request = twitter_api.TwitterFeeds(search_param=_unsearched)
            user_detail = twitter_api_request.fetch_user_detail_by_screen_name()
            # insert user detail
            try:
                db_util.MongoDBActor(constants.COLLECTIONS.USER_DETAILS).insert_data(user_detail)
                has_error=False,
                error=''
            except Exception as ex: # error
                continue
            # update the queue
            db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_QUEUE_DAILY_DOMAIN_SEARCH).find_and_modify(
                key={"username": _unsearched},
                data={"is_account_detail_processed": True, 'has_error': has_error, 'error':error})
            time.sleep(2)

    def get_time_lines_last_created_date(self, username):
        tl = db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_TIMELINESS).distinct(key="created_at",
                                                                                     filter={'username': username})
        if None in tl:
            tl.remove(None)

        if len(tl) == 0:
            return None

        tl.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.000Z"), reverse=True)
        print("Found timelines existed created:{}".format(list(tl)))
        return tl[0]

    def process_timelines_search(self):
        if self.search_keyword and self.domain:
            _unsearched_accounts = set(db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_QUEUE_DAILY_DOMAIN_SEARCH) \
                                       .distinct(key="username",
                                                 filter={'search_keyword': self.search_keyword,
                                                         "is_timelines_processed": False}))
        else:
            _unsearched_accounts = set()

            for val in db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_QUEUE_DAILY_DOMAIN_SEARCH).find():
                if 'username' in val and "is_timelines_processed" in val:
                    if val["is_timelines_processed"] is False:
                        _unsearched_accounts.add(val['username'])

        _official_twitter_accounts = self.get_official_twitter_accounts()
        for count, _unsearched_account in enumerate(_unsearched_accounts):
            _is_fetch_success = shared_util.fetch_user_if_not_present(_unsearched_account)
            if not _is_fetch_success:  # either blocked / not available account
                continue

            if _unsearched_account in _official_twitter_accounts:
                continue

            _is_account_verified = shared_util.is_account_verified(_unsearched_account)
            if _is_account_verified is None or _is_account_verified:  # exclude verified account / not found account
                continue

            _numeric_id = shared_util.get_user_id_from_user_detail(_unsearched_account)
            if not _numeric_id:  # chances are account is suspended
                continue
            may_be_last_time_line = self.get_time_lines_last_created_date(_unsearched_account)

            if may_be_last_time_line:
                print("Escaping, previously fetched user timelines ... {}".format(_unsearched_account))
                continue

            if may_be_last_time_line is not None:
                _timelines = twitter_api.TwitterFeeds().get_user_tweets(numeric_user_id=_numeric_id,
                                                                        _additional_query_param={
                                                                            'start_time': may_be_last_time_line
                                                                        })
            else:
                _timelines = twitter_api.TwitterFeeds().get_user_tweets(numeric_user_id=_numeric_id)
            for _tl in _timelines:
                _tl['username'] = _unsearched_account
                _tl['id'] = _numeric_id
                _tl['time'] = self.get_query_time()
                _insert = db_util.MongoDBActor(constants.COLLECTIONS.TWITTER_TIMELINESS).insert_data(_tl)
                print("Timeline inserted:{}, _insert_id:{}".format(_unsearched_account, _insert))
            time.sleep(15)


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Daily domain search")
    _arg_parser.add_argument("-d", "--domain",
                             action="store",
                             required=False,
                             help="domain name")
    _arg_parser.add_argument("-s", "--search_keyword",
                             action="store",
                             required=False,
                             help="search keyword")
    _arg_parser.add_argument("-t", "--timelines",
                             action="store",
                             required=False,
                             help="timelines")

    _arg_value = _arg_parser.parse_args()

    if _arg_value.domain and _arg_value.search_keyword and _arg_value.timelines:
        daily_search = SearchAccounts(domain=_arg_value.domain, search_keyword=_arg_value.search_keyword, timelines=_arg_value.timelines)
        daily_search.process()
    elif _arg_value.domain and _arg_value.search_keyword:
        daily_search = SearchAccounts(domain=_arg_value.domain, search_keyword=_arg_value.search_keyword)
        daily_search.process()
    elif _arg_value.timelines:
        daily_search = SearchAccounts(timelines=_arg_value.timelines)
        daily_search.process()
