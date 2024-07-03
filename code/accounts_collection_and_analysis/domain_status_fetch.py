import argparse

import shared_util
import db_util
import constants


class DomainStatus:
    def __init__(self, domain):
        self.domain = domain

    def process(self):
        _status_code = shared_util.get_domain_status("https://{}".format(self.domain))
        db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).find_and_modify(key={'domain': self.domain},
                                                                           data={
                                                                               'url_status_code': _status_code
                                                                           })
        print("Inserted {} {}".format(self.domain, _status_code))


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Process analysis of the data collected")
    _arg_parser.add_argument("-a", "--analyze_data",
                             action="store",
                             required=True,
                             help="Data processing name for analysis")

    _arg_value = _arg_parser.parse_args()

    if _arg_value.analyze_data == "process_all":
        print("Processing all domain url status code fetch")
        _domain_urls = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="domain"))
    else:
        print("Processing redo of unsucess status code")
        _domain_urls_with_unsucess = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="domain",
                                                                                       filter={'url_status_code': -1}
                                                                                       ))
        domain_urls_with_absence_code = set(db_util.MongoDBActor(constants.COLLECTIONS.DOMAIN).distinct(key="domain",
                                                                                                    filter={
                                                                                                        'url_status_code':
                                                                                                            {'$exists': False}}
                                                                                                    ))
        _domain_urls = _domain_urls_with_unsucess.union(domain_urls_with_absence_code)

    for counter, domain_ in enumerate(_domain_urls):
        print("Processing {}/{},{}".format(counter + 1, len(_domain_urls), domain_))
        _scraper = DomainStatus(domain_)
        _scraper.process()
