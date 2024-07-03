import os
import ast
from tqdm import tqdm
import random
from transformers import set_seed


from exp_utils import read_csv, write_csv, write_csv_cluster, extract_sentences_cluster, get_keywords

# -------------------------

PATH_CSV_DATA = 'experiments/results/BERTopic/all-mpnet-base-v2/hdbscan/full/23-01-2024_04-57-57/analysis/24-01-2024_02-26-31/reducted'

# -------------------------

PATH_FOLDER_DATA = 'data/all'

# -------------------------
# SELECTED CLUSTERED TOPICS
# -------------------------

TOPIC_ID = [205, 32, 121, 63, 119, 472, 204, 108, 450, 478, 73, 526, 285, 474, 259, 355, 269, 54, 29, 199, 417, 424, 163, 232, 390, 423, 104, 366, 353, 333, 161, 307, 514, 386, 516, 13, 129, 96, 158, 508, 490, 463, 448, 245, 267, 172, 376, 370, 497, 228, 91, 226, 309, 47, 295, 30, 445, 460, 237, 271, 412, 79, 148, 109, 38, 499, 467, 407, 243, 249, 513, 178, 36, 172, 385, 93, 24, 421, 496, 404, 248, 457, 208, 507, 95, 115, 320, 87, 393, 380, 175, 418, 72, 504, 98, 186, 137, 152, 21, 503, 9]

# -------------------------
# EXTRA, TO WORK IF AN UNIQUE FOLDER IS USED
# -------------------------

PATH_FOLDER_ORIGINAL_DATA = 'data_original'
DOUBLE_CHECK_LIST = ['data/youtube/BBCArchive.csv','data/youtube/BreakwaterIT.csv','data/youtube/CricketAus.csv', 'data/youtube/MSFTMechanics.csv', 'data/youtube/MyAmazonGuy.csv', 'data/youtube/RakeemAddison.csv', 'data/twitter/abbvie.csv', 'data/youtube/adidas.csv','data/youtube/alibabagroup.csv', 'data/youtube/aljazeeraenglish.csv', 'data/youtube/amazon.csv','data/twitter/appleinsider.csv','data/youtube/arstechnica.csv', 'data/youtube/aweber.csv','data/youtube/bbcthree.csv','data/youtube/bitrix24.csv','data/youtube/coxcommunications.csv','data/youtube/csis.csv','data/youtube/digikey.csv','data/youtube/digitaltrends.csv','data/youtube/dynatrace.csv','data/youtube/livemint.csv','data/youtube/mondaydotcom.csv','data/twitter/msdevru.csv','data/youtube/msnbc.csv','data/youtube/nike.csv','data/twitter/nintendolife.csv','data/youtube/pagesix.csv','data/youtube/realmadrid.csv','data/twitter/salesforcejobs.csv','data/youtube/seagate.csv','data/youtube/semrush.csv','data/youtube/siteground.csv','data/telegram/torproject.csv','data/youtube/unity.csv']

# -------------------------
# -------------------------

N_KEYWORDS = 5

# -------------------------
# TO CONSTRUCT NEW SAMPLED DATASET
# -------------------------

BUILD_SAMPLED_DATASETS = False
PATH_CLUSTERS_CSV_TO_SAVE = os.path.join(PATH_CSV_DATA, 'cluster_csv')

NEW_TEMPLATE = True
ONLY_ENG_POST = True
NOT_UNK_POST = False
REMOVE_DOUBLE_SPACE = True
NOT_INIT_WORDS = []
N_FILES = None
MAX_POST_PER_FILE = None

SEED = 1112

# -------------------------


def extract_results_BERTopic(path_csv_data, n_keywords=5):
    reduced_csv_file = os.path.join(path_csv_data, 'topic_info.csv')

    if not os.path.exists(reduced_csv_file):
        print('ERROR, reduced csv file not present')
    else:
        header, data = read_csv(reduced_csv_file)

        col_index_idx = header.index('Topic')
        col_index_count = header.index('Count')
        col_index_keybert = header.index('KeyBERT')

        header_filter = ['Cluster Id', 'Keywords', 'Count Posts', '% Posts', '% Posts without Miscellaneous']
        data_filter = []

        count_misc = 0
        count_tot = 0

        for riga in data:
            if int(riga[col_index_idx]) not in TOPIC_ID:
                count_misc += int(riga[col_index_count])
            count_tot += int(riga[col_index_count])

        data_filter.append([-1, "", count_misc, (count_misc / count_tot) * 100, 0])

        for riga in data:
            if int(riga[col_index_idx]) in TOPIC_ID:
                keywords = [elem_list for elem_list in ast.literal_eval(riga[col_index_keybert])][:n_keywords]
                str_keyword = ' | '.join(keywords)

                data_filter.append([int(riga[col_index_idx]), str_keyword, int(riga[col_index_count]),
                                    (int(riga[col_index_count]) / count_tot) * 100,
                                    (int(riga[col_index_count]) / (count_tot - count_misc)) * 100])

        csv_path_out = os.path.join(path_csv_data, 'results_summary.csv')
        write_csv(csv_path_out, header_filter, data_filter)


# -------------------------


def create_csv_headers_list(path_csv_data, path_folder_original_data=None, n_keywords=5, filter_list=None, double_check_list=None):
    reduced_csv_results = os.path.join(path_csv_data, 'reducted_results.csv')
    reduced_csv_file = os.path.join(path_csv_data, 'topic_info.csv')
    if not os.path.exists(reduced_csv_results) or not os.path.exists(reduced_csv_file):
        print('ERROR, reduced csv files not present')
    else:
        platform_dict_filename = {}
        if path_folder_original_data is not None:
            platform_folders = os.listdir(path_folder_original_data)
            for platform_folder in platform_folders:
                platform_dict_filename[platform_folder] = [os.path.join(root, file).split('/')[-1].split('.csv')[0] for
                                                           root, dirs, files in
                                                           os.walk(os.path.join(path_folder_original_data, platform_folder))
                                                           for file in files if file.endswith(".csv")]

        header_info, data_info = read_csv(reduced_csv_file)
        col_info_index_idx = header_info.index('Topic')
        col_info_index_keybert = header_info.index('KeyBERT')
        # ---
        header_results, data_results = read_csv(reduced_csv_results)
        col_results_index_file_name = header_results.index('file_name')
        col_results_index_csv_path = header_results.index('csv_path')
        col_results_index_label = header_results.index('label')
        # ---

        header_file_name_list = ['cluster_id', 'cluster_keywords', 'list_of_handles_csv']
        data_file_name_list = []

        #  all_data: {'cluster_id': {'cluster_keywords': [],
        #                           'list_of_handles_csv' : []}
        all_data = {}
        for riga_results in data_results:
            cluster_id = riga_results[col_results_index_label]
            if filter_list is None or int(cluster_id) in filter_list:
                file_name = riga_results[col_results_index_file_name]
                idx_double_check = None
                if path_folder_original_data is not None:
                    platform_final_name_list = []
                    for platform, platform_list_file_name in platform_dict_filename.items():
                        if file_name in platform_list_file_name:
                            platform_final_name_list.append(f'data/{platform}/{file_name}.csv')
                    if len(platform_final_name_list) != 1:
                        check_list = [path_file_name.split('/')[-1].split('.csv')[0] for path_file_name in
                                      double_check_list]
                        if file_name in check_list:
                            idx_double_check = check_list.index(file_name)
                        else:
                            idx_double_check = -1
                            print(f'-1 for {file_name}, continue')
                else:
                    platform_final_name_list = [f'{riga_results[col_results_index_csv_path]}/{file_name}.csv']
                if riga_results[col_results_index_label] not in all_data.keys():
                    str_keyword = get_keywords(data_info, cluster_id, col_info_index_idx, col_info_index_keybert, n_keywords)
                    if idx_double_check is None:
                        all_data[cluster_id] = {
                            'cluster_keywords': str_keyword,
                            'list_filename': [file_name],
                            'list_of_handles_csv': [platform_final_name_list[0]]
                        }
                    elif idx_double_check != -1:
                        all_data[cluster_id] = {
                            'cluster_keywords': str_keyword,
                            'list_filename': [file_name],
                            'list_of_handles_csv': [double_check_list[idx_double_check]]
                        }
                else:
                    list_filename_csv = all_data[cluster_id]['list_filename']
                    if file_name not in list_filename_csv:
                        if idx_double_check is None:
                            all_data[cluster_id]['list_filename'].append(file_name)
                            all_data[cluster_id]['list_of_handles_csv'].append(platform_final_name_list[0])
                        elif idx_double_check != -1:
                            all_data[cluster_id]['list_filename'].append(file_name)
                            all_data[cluster_id]['list_of_handles_csv'].append(double_check_list[idx_double_check])

        for cluster_id, value in all_data.items():
            data_file_name_list.append([cluster_id, value['cluster_keywords'], value['list_of_handles_csv']])

        if filter_list is not None:
            csv_path_out = os.path.join(path_csv_data, 'results_filtered_list_of_handles.csv')
        else:
            csv_path_out = os.path.join(path_csv_data, 'results_list_of_handles.csv')
        write_csv(csv_path_out, header_file_name_list, data_file_name_list)


def results_csv_header_list(path_csv_data, path_folder_original_data=None, n_keywords=5, filter_list=None, double_check_list=None):
    create_csv_headers_list(path_csv_data, path_folder_original_data=PATH_FOLDER_ORIGINAL_DATA, n_keywords=n_keywords, double_check_list=double_check_list)
    if filter_list is not None:
        create_csv_headers_list(path_csv_data, path_folder_original_data=PATH_FOLDER_ORIGINAL_DATA, n_keywords=n_keywords, filter_list=filter_list, double_check_list=double_check_list)


# -------------------------


def construct_sampled_dataset(path_csv_data, path_folder_data, path_to_save, n_max_sentence=None):
    if not os.path.exists(path_to_save):
        os.makedirs(path_to_save)
    reduced_csv_results = os.path.join(path_csv_data, 'reducted_results.csv')
    print(reduced_csv_results)
    if not os.path.exists(reduced_csv_results) or not os.path.exists(reduced_csv_results):
        print('ERROR, reduced csv files not present')
    else:
        header_results, data_results = read_csv(reduced_csv_results)
        col_results_index_idx = header_results.index('idx')
        col_results_index_file_name = header_results.index('file_name')
        col_results_index_label = header_results.index('label')
        # ---

        final_dict_filtered_dataset = {}
        for riga_results in tqdm(data_results, desc="Rows"):
            cluster_id_row = riga_results[col_results_index_label]
            post_id_row = int(riga_results[col_results_index_idx])
            if int(cluster_id_row) in TOPIC_ID:
                if cluster_id_row not in final_dict_filtered_dataset.keys():
                    final_dict_filtered_dataset[cluster_id_row] = []
                file_csv_account = os.path.join(path_folder_data,
                                                f"{riga_results[col_results_index_file_name]}.csv")
                with open(file_csv_account, "r", encoding="utf-8") as csv_file:
                    content = csv_file.read()

                    sentences_dict = extract_sentences_cluster(file_csv_account, content, max_post_per_files=None)
                    sentences = sentences_dict['sentence']
                    final_dict_filtered_dataset[cluster_id_row].append(sentences[post_id_row])
                    del sentences_dict

        for cluster_id, list_post in final_dict_filtered_dataset.items():
            if n_max_sentence is not None:
                if len(list_post) > n_max_sentence:
                    list_post = random.sample(list_post, n_max_sentence)
                path_to_save_random = os.path.join(path_to_save, "random")
                if not os.path.exists(path_to_save_random):
                    os.makedirs(path_to_save_random)
                csv_path_out = os.path.join(path_to_save_random, f"{cluster_id}.csv")
            else:
                csv_path_out = os.path.join(path_to_save, f"{cluster_id}.csv")
            final_list_post = []
            for elem in list_post:
                final_list_post.append("----start-----")
                final_list_post.append(elem)
                final_list_post.append("----end-----")

            write_csv_cluster(csv_path_out, final_list_post)


# -------------------------

def construct_sampled_datasets(path_csv_data, path_folder_data, path_to_save, n_max_sentence=None):
    print('All')
    construct_sampled_dataset(path_csv_data, path_folder_data, path_to_save, n_max_sentence=None)
    if n_max_sentence is not None:
        print('Random')
        construct_sampled_dataset(path_csv_data, path_folder_data, path_to_save, n_max_sentence=n_max_sentence)


def main():
    print('Summary results')
    extract_results_BERTopic(path_csv_data=PATH_CSV_DATA, n_keywords=N_KEYWORDS)
    print('Building csv headers list')
    results_csv_header_list(path_csv_data=PATH_CSV_DATA, path_folder_original_data=PATH_FOLDER_ORIGINAL_DATA, n_keywords=N_KEYWORDS, filter_list=TOPIC_ID, double_check_list=DOUBLE_CHECK_LIST)
    if BUILD_SAMPLED_DATASETS:
        print('Build new sampled datasets')
        construct_sampled_datasets(path_csv_data=PATH_CSV_DATA, path_folder_data=PATH_FOLDER_DATA, path_to_save=PATH_CLUSTERS_CSV_TO_SAVE, n_max_sentence=20)


if __name__ == "__main__":
    set_seed(SEED)
    main()