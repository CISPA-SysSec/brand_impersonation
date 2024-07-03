
import requests
import db_util
import shared_util
import constants
from bs4 import BeautifulSoup


class WebsiteScrapper:
    def __init__(self, _url):
        self._url = _url

    def process_email_scraping_from_domain(self):
        try:
            reqs = requests.get("https://{}".format(self._url), timeout=10)
            soup = BeautifulSoup(reqs.text, 'html.parser')
            if soup:
                soup = str(soup)
                print(soup)
            _potential_emails = shared_util.may_be_email_address_address_in_page_source(str(soup), self._url)
            return list(_potential_emails)
        except Exception as ex:
            _error_ = str(ex)
            print(_error_, self._url)
            return {'error': _error_}

    def _get_all_urls(self):
        try:
            reqs = requests.get("https://{}".format(self._url), timeout=10)
            soup = BeautifulSoup(reqs.text, 'html.parser')
        except Exception as ex:
            _error_ = str(ex)
            print(_error_, self._url)
            return {'error': _error_}

        _all_urls = set()
        for link in soup.find_all('a'):
            _all_urls.add(link.get('href'))
        return list(_all_urls)

    def process(self):
        _all_links = self._get_all_urls()
        _data_ = {}
        if None in _data_:
            _data_.remove(None)
        for _link in _all_links:
            if _link is None:
                continue
            if 'https://www.tiktok.com/' in _link:
                _data_['tiktok'] = self._replace_and_curate_str(_link, "https://www.tiktok.com/")
            elif 'https://www.instagram.com/' in _link:
                _data_['instagram'] = self._replace_and_curate_str(_link, "https://www.instagram.com/")
            elif 'https://twitter.com/' in _link:
                _data_['twitter'] = self._replace_and_curate_str(_link, "https://twitter.com/")
            elif 'https://www.facebook.com/' in _link:
                _data_['facebook'] = self._replace_and_curate_str(_link, 'https://www.facebook.com/')
            elif 'https://www.youtube.com/' in _link:
                _data_['youtube'] = self._replace_and_curate_str(_link, 'https://www.youtube.com/')
        return _data_

    def _replace_and_curate_str(self, _str, replace_by):
        _str = _str.replace("\n", "")
        _str = _str.replace(replace_by, "")
        _str = _str.replace("/", "")
        _str = _str.strip()
        return _str


if __name__ == "__main__":
    _domain_urls = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="domain"))
    for counter, _domain_url in enumerate(_domain_urls):
        _scraper = WebsiteScrapper(_domain_url)
        _found_data = _scraper.process_email_scraping_from_domain()
        db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).find_and_modify(key={'domain': _domain_url},
                                                                           data=_found_data)
        print("Processing {}/{}, {} --> {}".format(counter + 1, len(_domain_urls), _domain_url, _found_data))



