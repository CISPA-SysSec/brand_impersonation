# python experiments/run_img_sim.py

import torch
from sentence_transformers import util
import csv
from tqdm import tqdm
import os
import sys
from transformers import set_seed

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from src.utils.utilities import generate_time


# ------------------------

PATH_SAVED_IMG_ENC = 'data_img_enc/CLIP-VIT-B-16'

MODEL_NAME = PATH_SAVED_IMG_ENC.split('/')[-1]

print(MODEL_NAME)

# ------------------------

INIT_PATH_CSV = 'experiments/results/img_sim'
INIT_PATH_CSV = os.path.join(INIT_PATH_CSV, MODEL_NAME, generate_time())

if not os.path.exists(INIT_PATH_CSV):
    os.makedirs(INIT_PATH_CSV)

# ------------------------

SEED = 1112

# ------------------------

OFFICIAL_NAME = 'official'

# ------------------------

DEVICE = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

# ------------------------
# ------------------------

def generate_clip_score_from_encoded(enc_img1, enc_img2):
    cos_scores = util.pytorch_cos_sim(enc_img1, enc_img2)
    score = round(float(cos_scores[0][0])*100, 2)
    return score

# ------------------------
# ------------------------


def main():
    set_seed(SEED)

    print(f"Using {DEVICE}")

    folders_img_enc = [d for d in os.listdir(PATH_SAVED_IMG_ENC) if os.path.isdir(os.path.join(PATH_SAVED_IMG_ENC, d))]

    filtered_folders_img_enc = [item for item in folders_img_enc if item != OFFICIAL_NAME]

    full_official_folder_img_enc = os.path.join(PATH_SAVED_IMG_ENC, OFFICIAL_NAME)
    official_img_enc_files = [os.path.join(root, file) for root, dirs, files in os.walk(full_official_folder_img_enc) for
                              file in files if file.endswith(".pt")]

    list_official_enc_tensor = []

    for official_img_enc_file in official_img_enc_files:
        official_name = official_img_enc_file.split('/')[-1].split('.')[:-1][0]

        official_enc_tensor = torch.load(official_img_enc_file).to(DEVICE)
        list_official_enc_tensor.append((official_name, official_enc_tensor))

    for test_folder_img_enc in tqdm(filtered_folders_img_enc, desc="Folders", position=0):
        full_test_folder_img_enc = os.path.join(PATH_SAVED_IMG_ENC, test_folder_img_enc)
        test_enc_files = [os.path.join(root, file) for root, dirs, files in os.walk(full_test_folder_img_enc) for file in
                          files if file.endswith(".pt")]

        super_folder = os.path.join(INIT_PATH_CSV, test_folder_img_enc)
        if not os.path.exists(super_folder):
            os.makedirs(super_folder)

        header = ['file_name', 'best_official_name', 'best_official_score', 'complete_score_list']

        rows = [header, ]

        for test_enc_file in tqdm(test_enc_files, desc='Images', position=1):
            test_name = test_enc_file.split('/')[-1].split('.')[:-1][0]
            test_enc_tensor = torch.load(test_enc_file)

            all_official_clip_score = []

            for official_name, official_enc_tensor in list_official_enc_tensor:
                score = generate_clip_score_from_encoded(test_enc_tensor, official_enc_tensor)

                all_official_clip_score.append((official_name, score))

            all_official_clip_score = sorted(all_official_clip_score, key=lambda x: x[1], reverse=True)

            rows.append([test_name, all_official_clip_score[0][0], all_official_clip_score[0][1], all_official_clip_score])

        with open(os.path.join(super_folder, "results.csv"), 'w') as f:
            write = csv.writer(f)
            write.writerows(rows)


if __name__ == "__main__":
    main()