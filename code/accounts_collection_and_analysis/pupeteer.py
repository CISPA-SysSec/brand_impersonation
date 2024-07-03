from pyppeteer import launch
from db_util import MongoDBActor

import time
import shared_util


class PupeteerDriver:
    def __init__(self, _url_, additional_param={}):
        self._url_ = _url_
        self.additional_param = additional_param

    async def main(self):
        browser = await launch(options={'args': ['--no-sandbox']})
        page = await browser.newPage()
        await page.goto(self._url_)
        time.sleep(3)
        if 'img_path' in self.additional_param:
            await page.screenshot({'path': self.additional_param['img_path'], 'fullPage': True})
        page_content = await page.content()
        if 'store_result' in self.additional_param:
            _store_result = self.additional_param['store_result']
            if _store_result:
                if 'collection_name' in self.additional_param:
                    _collection_name = self.additional_param['collection_name']
                    MongoDBActor(_collection_name).insert_data({
                        'url': self._url_,
                        'page_source': page_content,
                        'img_path': self.additional_param['img_path'],
                        'img_text': shared_util.get_text_from_img(self.additional_param['img_path']),
                        'time': int(time.time() * 1000)
                    })
        await browser.close()