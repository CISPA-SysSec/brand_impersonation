import argparse
import shared_util

from selenium_driver import PageDriver
from db_util import MongoDBActor
from constants import COLLECTIONS


class SearchTelegramAccounts:
    def __init__(self, service_search):
        self.service_search = service_search

    def process(self):
        if self.service_search == "telemetr":
            self.process_telemetrio()
        elif self.service_search == "telegram_handle_meta_data":
            self.process_telegram_account_meta_data_populate()
        else:
            raise Exception("Unsupported search request!")

    def get_telegram_collected_handle(self):
        channel_handle = set(MongoDBActor(COLLECTIONS.TELEGRAM_CHANNELS).distinct(key="channel_handle"))
        if None in channel_handle:
            channel_handle.remove(None)
        if "" in channel_handle:
            channel_handle.remove("")
        return channel_handle

    def process_telegram_account_meta_data_populate(self):
        _channels = self.get_telegram_collected_handle()
        _all_channel_len = len(_channels)
        try:
            _page_driver = PageDriver("https://google.com")
            _driver = _page_driver.get_driver()
            for counter, channel in enumerate(_channels):
                print("Processing {}/{}, {}".format(counter, _all_channel_len, channel))
                is_already_processed = set(MongoDBActor(COLLECTIONS.TELEGRAM_CHANNELS).distinct(key="channel_handle", filter={
                    "channel_handle": channel,
                    "is_blocked": {'$exists': True}}))
                if len(is_already_processed) > 0:
                    print("Already processed, escaping {}/{}, {}".format(counter, _all_channel_len, channel))
                    continue
                _url = "https://telemetr.io/en/channels?channel={}".format(channel)
                _driver.get(_url)
                _page_driver.scroll_to_bottom_of_page()

                _get_all_href_and_text = _page_driver.try_getting_href_and_text()
                _page_url = None
                for val in _get_all_href_and_text:
                    if len(val) < 1:
                        continue
                    _link = val[0]
                    if _link is None:
                        continue

                    if '/en/channels/' not in _link:
                        continue

                    if channel not in _link:
                        continue

                    _last_path_ = _link.split("/")[-1]
                    _channel_splited_name = _last_path_.split("-")
                    if len(_channel_splited_name) != 2:
                        continue

                    _channel_splited_name = _channel_splited_name[1].strip()
                    if _channel_splited_name != channel:
                        continue
                    _page_url = "https://telemetr.io/en/channels/{}".format(_last_path_)
                    break

                if _page_url:
                    _single_data = self.scrape_single_channel_data(_driver, _page_driver, _page_url)
                    MongoDBActor(COLLECTIONS.TELEGRAM_CHANNELS).find_and_modify(key={"channel_handle": channel},
                                                                                data=_single_data)

            _page_driver.clean_up()
        except Exception as ex:
            print("Exception occurred {}".format(ex))

    def scrape_single_channel_data(self, _driver, _page_driver, _page_url):
        _data_ = {}
        try:
            # fetch url requested
            _driver.get(_page_url)
            _h2_channel_name_xpath = '//h2[@class="link channel-name"]'
            _block_div = "*//div[@class='ban-icon']/div"
            _category_xpath = "//div[@class='text-weight category-block']"
            _language_xpath = "//div[@class='localization-block']"
            _info_channel_header_xpath = "//h1[@class='channel-page-header']"
            _info_content_xpath = "//div[@class='info-block-content']"
            _show_more_link_text = "//a[contains(text(),'Show more')]"
            _channel_logo_xpath = "//img[@class='channel-avatar']"
            _subscriber_xpath = "//div[@class='short-statistics-block']/div[1]"

            _h2_element_name = _page_driver.try_waiting_xpath(_h2_channel_name_xpath)
            _block_div_element = _page_driver.try_waiting_xpath(_block_div, wait_time=1)

            if _block_div_element is not None:
                _data_tool_tip_attribute = _block_div_element.get_attribute('data-tooltip')
                if _data_tool_tip_attribute:
                    _data_['blocked_msg'] = _data_tool_tip_attribute.strip()
                    _data_['is_blocked'] = True
                else:
                    _data_['is_blocked'] = False
            else:
                _data_['is_blocked'] = False

            _show_more_element = _page_driver.try_waiting_xpath(_show_more_link_text, wait_time=1)
            if _show_more_element:
                _show_more_element.click()

            _category_element = _page_driver.try_waiting_xpath(_category_xpath, wait_time=1)
            if _category_element:
                _category_text = _category_element.text
                _category_text = _category_text.split("\n")[1]
                _data_['category'] = _category_text
            else:
                _data_['category'] = None

            _language_element = _page_driver.try_waiting_xpath(_language_xpath, wait_time=1)
            if _language_element:
                _language_text = _language_element.text
                _language_text = _language_text.split("\n")[1]
                _data_['language'] = _language_text
            else:
                _data_['language'] = None

            _info_header_element = _page_driver.try_waiting_xpath(_info_channel_header_xpath, wait_time=1)
            if _info_header_element:
                _info_header_text = _info_header_element.text
                _data_['info_header'] = _info_header_text
            else:
                _data_['info_header'] = None

            _info_content_element = _page_driver.try_waiting_xpath(_info_content_xpath, wait_time=1)
            if _info_content_element:
                _info_content_text = _info_content_element.text
                _data_['info_content'] = _info_content_text
            else:
                _data_['info_content'] = None

            _channel_logo_element = _page_driver.try_waiting_xpath(_channel_logo_xpath, wait_time=1)
            if _channel_logo_element:
                _channel_href_link = _channel_logo_element.get_attribute('src')
                if _channel_href_link:
                    if 'https://telemetr.io/' not in _channel_href_link:
                        _channel_href_link = "https://telemetr.io/{}".format(_channel_href_link)
                    _data_['logo_link'] = _channel_href_link
                else:
                    _data_['logo_link'] = None

            else:
                _data_['logo_link'] = None

            _subscriber_element = _page_driver.try_waiting_xpath(_subscriber_xpath, wait_time=1)
            if _subscriber_element:
                _subscriber_count = _subscriber_element.text
                _subscriber_count = _subscriber_count.split("\n")[0]
                _data_['subscriber'] = _subscriber_count
            else:
                _data_['subscriber'] = None
        except:
            pass

        return _data_

    def process_telemetrio(self):
        _url = "https://telemetr.io/en/channels?channel={}&page={}"
        _brand_names = shared_util.get_all_domain_name()
        try:
            _page_driver = PageDriver("https://bhupendraacharya.com")
            _driver = _page_driver.get_driver()
            for counter, _name in enumerate(_brand_names):
                print("Processing {}/{}, {}".format(counter, len(_brand_names), _name))
                self._get_data_from_url(_driver, _page_driver, _url, _name)
            _page_driver.clean_up()
        except Exception as ex:
            print("Exception occurred {}".format(ex))

    def _get_data_from_url(self, _driver, _page_driver, _url, _name):
        _all_found = []
        try:
            counter = 1
            do_continue = True
            while do_continue:
                _list_prev_size = len(_all_found)
                _driver.get(_url.format(_name, counter))
                _h1_telegram_channel_rating = '/html/body/div[4]/div[1]/div[1]/div[1]/h1'
                _h1_element = _page_driver.try_waiting_xpath(_h1_telegram_channel_rating)
                _page_driver.scroll_to_bottom_of_page()
                if not _h1_element:
                    continue
                _get_all_href_and_text = _page_driver.try_getting_href_and_text()
                if _get_all_href_and_text is None:
                    continue

                _new_found = []
                for _found_info in _get_all_href_and_text:
                    _link = _found_info[0]
                    _text = _found_info[1]

                    if _text is None:
                        continue

                    if _link is None:
                        continue

                    if '/en/channels/' not in _link:
                        continue

                    # Text split the number
                    # https://telemetr.io/en/channels/1743795444-amazon_deals_flipkart_netflix
                    if "-" in _link:
                        _split = _link.split("-")
                        if len(_split) > 1:
                            _link = _link.split("-")[1]

                    _data = {
                        'channel_name': _text,
                        'channel_handle': _link,
                        'source': 'telemetr',
                        'search_text': _name
                    }
                    print(_data)

                    if _data not in _all_found:
                        _all_found.append(_data)
                    MongoDBActor(COLLECTIONS.TELEGRAM_CHANNELS).find_and_modify(key={"channel_handle": _link}, data=_data)

                if len(_all_found) == _list_prev_size:
                    do_continue = False
                counter = counter + 1

        except Exception as ex:
            print("Exception occurred {}".format(ex))


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Telegram metadata fetch")
    _arg_parser.add_argument("-s", "--service_search",
                             action="store",
                             required=True,
                             help="processing service name")

    _arg_value = _arg_parser.parse_args()

    _telegram_search = SearchTelegramAccounts(_arg_value.service_search)
    _telegram_search.process()
