import cv2
from skimage import metrics

from tqdm import tqdm

import torch
import open_clip
import cv2
from sentence_transformers import util
from PIL import Image

import os
from tqdm import tqdm

from transformers import set_seed

# ------------------------

# default
# MODEL_NAME = 'ViT-B-16-plus-240'
# MODEL_NAME = 'CLIP-VIT-H-14'
# new: CLIP-VIT-B-16
MODEL_NAME = 'CLIP-VIT-B-16'


# ------------------------

PATH_DATA_IMG = './data_img'

PATH_SAVED_IMG_ENC = f'./data_img_enc/{MODEL_NAME}'

if not os.path.exists(PATH_SAVED_IMG_ENC):
    os.makedirs(PATH_SAVED_IMG_ENC)

# ------------------------

RESIZE = True
IMAGE_SIZE = (224, 224)

# ----------------------

N_MAX_PNG = None

# ------------------------
# ------------------------

DEVICE = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

# ------------------------

SEED = 1112

# ------------------------


def clip_set_up_model_and_device(model_name):
    # maybe also: encode_kwargs = {'normalize_embeddings': False}

    if model_name == 'ViT-B-16-plus-240':
        model, _, preprocess = open_clip.create_model_and_transforms('ViT-B-16-plus-240', pretrained="laion400m_e32")
    elif model_name == 'CLIP-VIT-H-14':
        # MODEL_NAME = 'hf-hub:laion/CLIP-ViT-H-14-laion2B-s32B-b79K'
        model, _, preprocess = open_clip.create_model_and_transforms('hf-hub:laion/CLIP-ViT-H-14-laion2B-s32B-b79K')
    elif model_name == 'CLIP-VIT-B-16':
        print(model_name)
        # MODEL_NAME = 'hf-hub:laion/CLIP-ViT-B-16-laion2B-s34B-b88K'
        model, _, preprocess = open_clip.create_model_and_transforms('hf-hub:laion/CLIP-ViT-B-16-laion2B-s34B-b88K')
    else:
        model, _, preprocess = open_clip.create_model_and_transforms(model_name)

    model.to(DEVICE)

    return model, preprocess


def clip_encode(model, preprocess, image, resize=False, new_size=(128, 128)):
    img = Image.open(image).convert("RGBA")

    if resize:
        img = img.resize(new_size)

    img = preprocess(img).unsqueeze(0).to(DEVICE)

    img = model.encode_image(img)
    return img


def generate_clip_score_from_encoded(enc_img1, enc_img2):
    cos_scores = util.pytorch_cos_sim(enc_img1, enc_img2)
    score = round(float(cos_scores[0][0]) * 100, 2)
    return score

# ------------------------
# ------------------------


def main():
    set_seed(SEED)

    print(f"Using {DEVICE}")

    model, preprocess = clip_set_up_model_and_device(MODEL_NAME)

    folders_img = [d for d in os.listdir(PATH_DATA_IMG) if os.path.isdir(os.path.join(PATH_DATA_IMG, d))]

    for folder_img in tqdm(folders_img, desc="Folders", position=0):
        full_path_img = os.path.join(PATH_DATA_IMG, folder_img)

        full_path_img_enc = os.path.join(PATH_SAVED_IMG_ENC, folder_img)

        if not os.path.exists(full_path_img_enc):
            os.makedirs(full_path_img_enc)

        png_files = [os.path.join(root, file) for root, dirs, files in os.walk(full_path_img) for file in files if
                     file.endswith(".png")]

        png_files = png_files[:N_MAX_PNG]

        for png_file in tqdm(png_files, desc="Images", position=1):
            png_name = png_file.split('/')[-1].split('.')[:-1][0]
            try:
                png_file_enc = clip_encode(model, preprocess, png_file, resize=RESIZE, new_size=IMAGE_SIZE)
                torch.save(png_file_enc, os.path.join(full_path_img_enc, f"{png_name}.pt"))
            except Exception as e:
                print(e)
                print(f"{folders_img}, {png_file}, ERROR 2")
                continue


if __name__ == "__main__":
    main()


