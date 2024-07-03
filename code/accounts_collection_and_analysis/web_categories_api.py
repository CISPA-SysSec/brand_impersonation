import requests
import constants
import db_util
import time


class GetDomainData:
    def __init__(self, domain):
        self.klazify_url = "https://www.klazify.com/api/categorize"
        self.domain = domain

    def process(self):
        payload = {"url": self.domain}
        headers = {
            'Accept': "application/json",
            'Content-Type': "application/json",
            'Authorization': 'Bearer {}'.format(constants.THIRD_PARTY_APIS.KLAZIFY_TOKEN),
            'cache-control': "no-cache"
        }
        response = requests.post(self.klazify_url, json=payload, headers=headers)
        return response.json()


if __name__ == "__main__":
    _domains = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="domain"))
    if None in _domains:
        _domains.remove(None)

    for counter,_domain in enumerate(_domains):
        _found = db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="domain", filter={
            'klazify_data': {'$exists': True},
            'domain': _domain
        })

        if len(_found) > 0:
            print("Already processed continuing")
            continue

        _req = GetDomainData("https://{}".format(_domain))
        _data = _req.process()

        db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).find_and_modify(key={"domain": _domain}, data={
            'klazify_data': _data
        })
        time.sleep(0.5)
        print("\n{},{},{}\n".format(counter, _domain, _data))
