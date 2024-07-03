import argparse

from pyppeteer import launch
from db_util import MongoDBActor
import constants
import asyncio
import time


class PupeteerDriver:
    def __init__(self, username, _url_, social_media):
        self.username = username
        self._url_ = _url_
        self.social_media = social_media

    async def main(self):
        try:
            browser = await launch(options={'args': ['--no-sandbox']})
            page = await browser.newPage()
            await page.goto(self._url_, timeout=3000)
            page_content = await page.content()
            _data_ = {
                'username': self.username,
                'page_source': page_content,
                'social_media': self.social_media
            }

            if 'Geld mit PayPal.Me senden' in page_content:
                is_found = True
                _name = page_content.split('Geld mit PayPal.Me senden')[0]
                _name = _name.split('content="')[1]
                _name = _name.strip()
                _data_['name'] = _name
            else:
                is_found = False
            _data_['is_found'] = is_found
            MongoDBActor(constants.COLLECTIONS.PAYPAL_INFO).insert_data(_data_)
            await browser.close()

        except Exception as ex:
            print("Exception occurred {}, {}".format(ex, self._url_, self.social_media))


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Payapl Data fetch")
    _arg_parser.add_argument("-s", "--social_media",
                             action="store",
                             required=True,
                             help="social media to process")

    _arg_value = _arg_parser.parse_args()
    _social_media = _arg_value.social_media
    _social_media = _social_media.strip()
    _accounts = set()

    if _social_media == "twitter" or _social_media == "instagram" or _social_media == "youtube":
        _official = set(MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key=_social_media))
    elif _social_media == "telegram":
        _official = set(MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="name"))
    elif _social_media == "all":
        _official_t = set(MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key='twitter'))
        _official_i = set(MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key='instagram'))
        _official_y = set(MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key='youtube'))
        _official_tl = set(MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="name"))
        _official = _official_t.union(_official_i).union(_official_y).union(_official_tl)
    else:
        raise Exception("Unsupported social media")

    official_ = set()
    for val in _official:
        if val:
            official_.add(val.lower())

    if _social_media == "all":
        for sm in ["twitter", "instagram", "telegram", "youtube"]:
            for val in MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find({"social_media": sm}):
                if 'search_str' in val:
                    _srch = val['search_str']
                    if _srch:
                        _accounts.add(_srch)
    else:
        for val in MongoDBActor(constants.COLLECTIONS.TXT_SIMILARITY).find({"social_media": _social_media}):
            if 'search_str' in val:
                _srch = val['search_str']
                if _srch:
                    _accounts.add(_srch)
    _accounts = _accounts.difference(official_)
    _len = len(_accounts)

    for cnt, _acc in enumerate(_accounts):
        _found = set(MongoDBActor(constants.COLLECTIONS.PAYPAL_INFO).distinct(key="username", filter={
            "username": _acc}))
        if len(_found) > 0:
            print("Already escaping {}/{} {}".format(cnt, _len, _acc))
            continue
        print("Processing {}/{} {}".format(cnt, _len, _acc))
        _url = "https://www.paypal.com/paypalme/{}".format(_acc)
        asyncio.get_event_loop().run_until_complete(
            PupeteerDriver(_acc, _url, _social_media).main())
        time.sleep(5)  # graceful wait, don't be too greedy
