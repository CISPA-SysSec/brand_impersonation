import time
import shared_util
from constants import COLLECTIONS
from db_util import MongoDBActor
from you_tube_data_scrapper import YouTubeData


class SearchYouTubeAccounts:
    def __init__(self, domain_name):
        self.domain_name = domain_name

    def is_you_tube_search_context_already_processed(self, search_context):
        _data = MongoDBActor(COLLECTIONS.YOUTUBE_VIDEO_SEARCH).find(key={'search_context': search_context})
        for _d in _data:
            if 'search_context' in _d:
                return True
        return False

    def _search_contexts(self):
        _search_contexts = [
            "{}".format(self.domain_name),
            "{} support".format(self.domain_name),
            "{} help".format(self.domain_name),
            "make money via {}".format(self.domain_name)
        ]
        return _search_contexts

    def process(self):
        for _srch_context in self._search_contexts():
            is_processed = self.is_you_tube_search_context_already_processed(_srch_context)
            if is_processed:
                print('Already processed, escaping {}'.format(_srch_context))
                continue
            _data_search = YouTubeData(_srch_context)
            _data_results = _data_search.search_video()
            for _data_ in _data_results:
                _data_['search_context'] = _srch_context
                _data_['domain_name'] = self.domain_name
                _data_['time'] = int(time.time() * 1000)
                MongoDBActor(collection_name=COLLECTIONS.YOUTUBE_VIDEO_SEARCH).insert_data(_data_)
                print("Data inserted Search context:{},\nDomain Name:{}, \nFound Data:{}".format(_srch_context,
                                                                                                     self.domain_name,
                                                                                                     _data_))


if __name__ == "__main__":
    _domains = shared_util.get_candidate_curated_domains()
    for _domain in _domains:
        _name = shared_util.get_name_from_domain(_domain)
        if _name:
            _search = SearchYouTubeAccounts(_domain)
            _search.process()
            time.sleep(1)
