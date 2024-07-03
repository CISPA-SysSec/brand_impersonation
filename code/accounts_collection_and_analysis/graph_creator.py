import argparse
import seaborn as sns
import json
from statistics import median
import matplotlib.pyplot as plt
import pandas as pd

sns.set(font_scale=1.5)
sns.set_theme()


class CreateGraph:
    def __init__(self, function):
        self.function = function

    def process(self):
        if self.function == "graph_combo_squatting_type_of_sequence_data":
            self.graph_combo_squatting_type_of_sequence_data()
        elif self.function == "graph_combo_squatted_top_words":
            self.graph_combo_squatted_top_words()
        elif self.function == "create_account_creation_cdf":
            self.create_account_creation_cdf()
        elif self.function == "followers_metrics":
            self.followers_metrics()
        elif self.function == "posts_metrics":
            self.posts_metrics()
        elif self.function == "top_twenty_brands":
            self.top_thirty_brands()
        elif self.function == "web_category_squatting":
            self.web_category_squatting()

    def web_category_squatting(self):
        with open("report/attributes/web_category_count/detail.json", "r") as f_read:
            data = json.load(f_read)

        csv_lines = ["Category,Squatting,Count"]
        for val in data:
            csv_lines.append("{},{},{}".format(val[0], 'Typosquatting', val[1]))
            csv_lines.append("{},{},{}".format(val[0], 'Combosquatting', val[2]))
            csv_lines.append("{},{},{}".format(val[0], 'Fuzzysquatting', val[3]))

        with open("report/graph_data/web_category_squatting.csv", "w") as f_write:
            for val in csv_lines:
                f_write.write(val + "\n")

        data_frame = pd.read_csv("report/graph_data/web_category_squatting.csv")
        gfg = sns.barplot(x=data_frame['Category'], y=data_frame['Count'], hue=data_frame['Squatting'])
        gfg.set_xticklabels(gfg.get_xticklabels(), rotation=90)
        gfg.set(xlabel='Web Categories', ylabel='Count')

        plt.gcf().set_size_inches(12, 7)
        plt.legend()
        save_path = "report/graph_pic/web_category.png"
        plt.savefig(save_path, bbox_inches="tight")

    def top_thirty_brands(self):
        with open("report/attributes/brands_targeted_count/detail.json", "r") as f_read:
            lines = json.load(f_read)
        _x = []
        _y = []
        line_cnt = 0
        for count, line in enumerate(lines):
            if len(line[0]) < 6:
                continue

            if line_cnt == 30:
                break

            print("{}, {}, {}".format(count, line[0], line[4]))
            _x.append(line[0])
            _y.append(line[4])
            line_cnt = line_cnt + 1
        gfg = sns.barplot(x=_x, y=_y)
        # line graph
        gfg.set_xticklabels(gfg.get_xticklabels(), rotation=90)
        gfg.set(xlabel='Top 30 Brands', ylabel='Count')
        plt.gcf().set_size_inches(12, 7)

        # plt.show()
        plt.legend()
        save_path = "report/graph_pic/top_thirty_brands.png"
        plt.savefig(save_path, bbox_inches="tight")

        with open("report/graph_data/top_30_brand_target.json", "w") as f_write:
            json.dump({'x': _x, 'y': _y}, f_write, indent=4)

    def posts_metrics(self):
        with open("report/attributes/public_metrics/posts.json", "r") as f_read:
            data = json.load(f_read)

        # line graph
        gfg = sns.ecdfplot(data)
        gfg.set(xlabel='Posts', ylabel='Count')
        save_path = "report/graph_pic/posts_graph.png"
        plt.savefig(save_path, bbox_inches="tight")

    def followers_metrics(self):
        with open("report/attributes/public_metrics/followers.json", "r") as f_read:
            data = json.load(f_read)

        # line graph
        gfg = sns.ecdfplot(data)
        gfg.set(xlabel='Followers', ylabel='Count')
        plt.xscale('log')
        # plt.legend()
        save_path = "report/graph_pic/followers_graph.png"
        plt.savefig(save_path, bbox_inches="tight")

    def create_account_creation_cdf(self):
        with open("report/attributes/account_creation/data.json", "r") as f_read:
            data = json.load(f_read)

        _aggregated_social_media = {}
        _aggregated_year = {}
        _all_accounts = set()
        for social_media, years in data.items():
            _sm = {}
            for each_year, handle in years.items():
                _all_accounts = _all_accounts.union(handle)
                year = int(each_year)
                _handle_count = len(handle)
                if year not in _aggregated_year:
                    _aggregated_year[year] = _handle_count
                else:
                    _aggregated_year[year] = _handle_count + _aggregated_year[year]

            _aggregated_social_media[social_media] = _sm
        _each_year_values = list(_aggregated_year.values())
        print("Total accounts:{}".format(len(_all_accounts)))
        print("Media by year:{}".format(median(_each_year_values)))

        gfg = sns.lineplot(_aggregated_social_media)
        gfg.set(xlabel='Year of Creation', ylabel='Count')
        plt.legend()
        save_path = "report/graph_pic/account_creation_line_graph.png"
        plt.savefig(save_path, bbox_inches="tight")

        with open("report/graph_data/account_creation_data.json", "w") as f_write:
            json.dump(_aggregated_social_media, f_write, indent=4)

    def graph_combo_squatted_top_words(self):
        with open("report/graph_data/combo_squatted_top_words.json", "r") as f_read:
            data = json.load(f_read)

        sns.barplot(data, orient='h', legend=False)
        plt.legend()
        plt.show()

    def graph_combo_squatting_sequence_length(self):
        with open("report/graph_data/combo_squatting_sequence_length_data.json", "r") as f_read:
            data = json.load(f_read)

        _ranges = []
        _official_account = []
        _search_account = []

        _recreated = {}
        for each in data:
            _recreated[each['sequence_length']] = each['search_account']
            _ranges.append(each['sequence_length'])
            _official_account.append(each['official_account'])
            _search_account.append(each['search_account'])

        _data_list = [
            # (_ranges, 'Sequence Length'),
            (_official_account, 'Brand Accounts'),
            (_search_account, 'Combosquatted Accounts')
        ]
        save_path = "report/graph_pic/graph_combo_squatting_sequence_length.png"
        sns.ecdfplot(data=pd.DataFrame(_recreated))
        plt.legend()
        plt.show()

    def graph_combo_squatting_type_of_sequence_data(self):
        save_path = "report/graph_pic/graph_combo_squatting_type_of_sequence_data.png"

        data = [67831, 342892, 230416, 298247]
        labels = ['Digit', 'Alpha Num', 'English Word', 'Others']
        palette_color = sns.color_palette('bright')
        plt.pie(x=data, labels=labels, colors=palette_color, autopct='%.0f%%')
        plt.savefig(save_path, bbox_inches="tight")

    def graph_cdf(self, _data_list, x_label, y_label, f_name, xscale_log=True, _plot=None):
        for val in _data_list:
            sns.ecdfplot(data=val[0], label=val[1], linewidth=3)
            plt.xlim([1, len(val[0])])

        plt.xlabel(x_label)
        plt.ylabel(y_label)
        if xscale_log:
            plt.xscale('log')
        plt.legend()
        plt.tight_layout()
        plt.savefig(f_name, bbox_inches='tight', pad_inches=0.1)
        plt.clf()
        plt.close(f_name)


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Graph creator")
    _arg_parser.add_argument("-f", "--function",
                             action="store",
                             required=True,
                             help="processing function name")

    _arg_value = _arg_parser.parse_args()

    Graph_ = CreateGraph(_arg_value.function)
    Graph_.process()
