import os

import random

import re

from tqdm import tqdm
from transformers import set_seed

from datasets import Dataset

import torch

import pycld2 as cld2

from sentence_transformers import SentenceTransformer

from collections import Counter
from keybert import KeyBERT

from collections import OrderedDict

#-----------------

# for the incorporation model to be chosen, see: https://www.sbert.net/_static/html/models_en_sentence_embeddings.html
# as a first prove I chose the following one, because it's multilingual
# MODEL_NAME = 'sentence-transformers/paraphrase-multilingual-mpnet-base-v2'
# MODEL_NAME = 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2'#
MODEL_NAME = 'sentence-transformers/all-mpnet-base-v2'

INPUT_DIRECTORY = "data/all"
OUTPUT_DIRECTORY = f"embeddings_15/{MODEL_NAME.split('/')[-1]}"

#-----------------

NEW_TEMPLATE = True

#-----------------

# If True -> select only posts in english
ONLY_ENG_POST = True

# If True -> do not select posts which is not possible to determine the language
NOT_UNK_POST = False

# If True -> remove from the sentences double spaces, and put a single space instead
REMOVE_DOUBLE_SPACE = True

# Each words in the posts that will start with an element in the follwoing list will be filtered from the sentence to create the embeddings,
# EX. if: NOT_INIT_WORDS = ['#', ...] -> an original sentence as:"Hello #Monica from Paris", will become: "Hello from Paris"
# OR EX. if: NOT_INIT_WORDS = ['http', ...] -> an original sentence as:"Click on https://hello.com now!", will become: "Click on now!"
NOT_INIT_WORDS = []

#-----------------

BATCH_SIZE = 128

# max number of csv to process. If None: process ALL
N_FILES = None

# max number of post per csv to process. If None: process ALL
MAX_POST_PER_FILE = 15

#-----------------

SEED = 1112

#-----------------

SAVE_ALSO_SENTENCES = True
NOT_INIT_WORDS_AFTER_EMBEDDINGS = ['http']

#-----------------

SAVE_ALSO_KEYWORDS_LIST = True

MAX_LEN_KEYWORDS_LIST = 100
TOP_N_KEYWORDS = 10
KEYPHRASE_NGRAM_RANGE = (1, 2)
REMOVE_DUPLICATE_WORDS = True
NOT_INIT_KEYWORDS_LIST = []
# NOT_INIT_KEYWORDS_LIST = ['http', ' rt ']

#-----------------

DEVICE = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")


#-----------------
#-----------------

def remove_duplicate_words(input_string):
    words = input_string.split()
    unique_words = list(OrderedDict.fromkeys(words))
    result_string = ' '.join(unique_words)
    return result_string

def extract_topkeywords_from_docs(model_name, docs, doc_embeddings, top_n_keywords, max_len, keyphrase_ngram_range):

    keybert_model = KeyBERT(model_name)
    docs_keywords = keybert_model.extract_keywords(docs=docs, doc_embeddings=doc_embeddings, top_n=top_n_keywords, keyphrase_ngram_range=keyphrase_ngram_range)

    if isinstance(docs_keywords, list) and not any(isinstance(sublist, list) for sublist in docs_keywords):
        docs_keywords = [docs_keywords]

    flat_list = [item[0] for sublist in docs_keywords for item in sublist]

    word_counts = Counter(flat_list)
    result_list = list(word_counts.items())
    result_list = sorted(result_list, key=lambda x: x[1], reverse=True)

    selected_words = [word[0] for word in result_list[:max_len]]
    result_string = ' '.join(selected_words)

    if REMOVE_DUPLICATE_WORDS:
        result_string = remove_duplicate_words(result_string)

    for not_word in NOT_INIT_KEYWORDS_LIST:
        result_string = re.sub(fr'\b{re.escape(not_word)}\S*\b', '', result_string, flags=re.MULTILINE).strip()

    return result_string

def process_sentences_after_embedding(sentences, not_word):
    for i in range(len(sentences)):
        sentences[i] = re.sub(fr'\b{re.escape(not_word)}\S*\b', '', sentences[i], flags=re.MULTILINE).strip()

    return sentences

def set_up_model_and_device(model_name):
    # maybe also: encode_kwargs = {'normalize_embeddings': False}

    model = SentenceTransformer(model_name).to(DEVICE)

    return model

def extract_sentences(file_path, content, max_post_per_files = None):
    if NEW_TEMPLATE:
        sentences = content.split("----end-----")
        sentences = [s.strip().replace("----start-----\n","").replace(";","").replace('\n', ' ') for s in sentences if s.strip()]
    else:
        sentences = content.split("-------- line separator --------")
        sentences = [s.strip().replace(";","").replace('\n', ' ') for s in sentences if s.strip()]

    # verify if it is necessary to be used also here...
    # random.shuffle(sentences)

    sentences_dict = {"idx": [], "csv_path": [], "sentence": []}

    for idx, sentence in enumerate(sentences[:max_post_per_files]):

        try:

            if NOT_UNK_POST:
                isReliable, textBytesFound, details = cld2.detect(sentence)
                if details[0][1] == "un":
                    continue

            if ONLY_ENG_POST:
                isReliable, textBytesFound, details = cld2.detect(sentence)
                if details[0][1] != "en":
                    continue

            for not_word in NOT_INIT_WORDS:
                sentence = re.sub(fr'{not_word}\S+', '', sentence, flags=re.MULTILINE).strip()

            if REMOVE_DOUBLE_SPACE:
                sentence = re.sub(' +', ' ', sentence)
        
        except Exception as e:
            continue

        sentences_dict['idx'].append(idx)
        sentences_dict['csv_path'].append(file_path)
        sentences_dict['sentence'].append(sentence)
    
    return sentences_dict


def process_data_and_store_embedding(sentences_dict, output_folder, model):
    dataset = Dataset.from_dict(sentences_dict)

    if dataset.num_rows > 0:
        upper_folder = '/'.join(dataset['csv_path'][0].split('/')[:-1])
        file_name = dataset['csv_path'][0].split('/')[-1].split('.csv')[0]

        if not os.path.exists(f'{output_folder}/{upper_folder}'):
            os.makedirs(f'{output_folder}/{upper_folder}')
        
        embeddings = model.encode(dataset['sentence'], batch_size=BATCH_SIZE, show_progress_bar=False, convert_to_tensor=True).cpu()

        std = torch.std(embeddings, dim=0).unsqueeze(0)
        mean = torch.mean(embeddings, dim=0).unsqueeze(0)

        if SAVE_ALSO_SENTENCES:
            sentences = dataset['sentence']
            for not_word in NOT_INIT_WORDS_AFTER_EMBEDDINGS:
                sentences = process_sentences_after_embedding(sentences, not_word)
            saved_sentences = sentences
        else:
            saved_sentences = None

        if SAVE_ALSO_KEYWORDS_LIST:
            keywords_sentence = extract_topkeywords_from_docs(MODEL_NAME, dataset['sentence'], embeddings, top_n_keywords=TOP_N_KEYWORDS, max_len=MAX_LEN_KEYWORDS_LIST, keyphrase_ngram_range=KEYPHRASE_NGRAM_RANGE)
        else:
            keywords_sentence = None

        # save embeddings dict
        torch.save({
            "tot_idx": len(dataset["idx"]),
            "csv_path": upper_folder,
            "file_name": file_name,
            "std": std,
            "mean": mean,
            "keywords_list": keywords_sentence,
            "sentences": saved_sentences,
            "embeddings": embeddings
        }, f'{output_folder}/{upper_folder}/{file_name}.pt')


def extract_sentences_create_embeddings(input_directory, output_directory, model, n_files = None, max_post_per_files = None):
    # Loop through CSV files to extract sentences and save to data.csv
    
    csv_files = [os.path.join(root, file) for root, dirs, files in os.walk(input_directory) for file in files if file.endswith(".csv")]

    # Shuffle the list in-place -> dovrei farla se non e' None
    random.shuffle(csv_files)

    for file_path in tqdm(csv_files[:n_files], desc="Extracting Sentences"):
        # print(file_path)
        
        with open(file_path, "r", encoding="utf-8") as csv_file:
            content = csv_file.read()
            
            sentences_dict = extract_sentences(file_path, content, max_post_per_files=max_post_per_files)
            process_data_and_store_embedding(sentences_dict, output_directory, model)

            del sentences_dict

def main():
    set_seed(SEED)

    print(f"Using {DEVICE}")

    model = set_up_model_and_device(MODEL_NAME)

    extract_sentences_create_embeddings(INPUT_DIRECTORY, OUTPUT_DIRECTORY, model, n_files = N_FILES, max_post_per_files = MAX_POST_PER_FILE)

if __name__ == "__main__":
    main()