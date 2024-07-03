import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import os
from tqdm import tqdm
import random
from transformers import set_seed
import sys

from PIL import Image

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from exp_utils import read_csv, write_csv

# -------------------------

PATH_CSV_DATA = 'experiments/results/img_sim/CLIP-VIT-B-16/10-01-2024_18-03-58'
THR_VALUE = 80
SET_THR_VALUE = 88
N_IMGS = 5

SEED = 112

# -------------------------

USE_LIST = True
RANDOM_IMGS_LOGO_1 = ['data_img/telegram/telegram_netflix_xyz.png', 'data_img/telegram/telegram_netflixnetflix2.png', 'data_img/twitter/twitter_NetflixGamers.png', 'data_img/twitter/twitter_netflixnovedad.png', 'data_img/telegram/telegram_web_series_hub_netflix.png']
RANDOM_IMGS_LOGO_2 = ['data_img/twitter/twitter_SamsungBoys.png', 'data_img/telegram/telegram_powerbankwithdraw1.png', 'data_img/twitter/twitter_SamsungTeam3.png', 'data_img/twitter/twitter_SamsungHDD.png', 'data_img/twitter/twitter_Samsung_Apk.png']
RANDOM_IMGS_LOGO_3 = ['data_img/twitter/twitter_DaveWymbsluv1.png', 'data_img/twitter/twitter_herrera_harley.png', 'data_img/telegram/telegram_binancekodcodeplase.png', 'data_img/telegram/telegram_No20twjTCUxlMTZi.png', 'data_img/twitter/twitter_Binance49706731.png']
RANDOM_IMGS_LOGO_4 = ['data_img/telegram/telegram_gift_cards_amazon_ebay.png', 'data_img/instagram/instagram_closezilla.png', 'data_img/twitter/twitter_AmznFulfillment.png', 'data_img/telegram/telegram_amazon_lootss.png', 'data_img/telegram/telegram_ott_updates_channel.png']
RANDOM_IMGS_LOGO_5 = ['data_img/twitter/twitter_expeukdeals.png', 'data_img/twitter/twitter_ExpediaAT.png', 'data_img/twitter/twitter_expediaseattle.png', 'data_img/twitter/twitter_ExpediaNO.png', 'data_img/twitter/twitter_ExpeFareAlert.png']
RANDOM_IMGS_LOGO_6 = ['data_img/twitter/twitter_WaImartOfficiaI.png', 'data_img/twitter/twitter_WalmartAssist.png', 'data_img/twitter/twitter_WalmartGamingUS.png', 'data_img/twitter/twitter_WalmartElectron.png', 'data_img/twitter/twitter_WalmartHealthy.png']
RANDOM_IMGS_LOGO_7 = ['data_img/telegram/telegram_AW4al87cQxcyMzUx.png', 'data_img/telegram/telegram_metamask_support8o.png', 'data_img/telegram/telegram_metamask_channel1.png', 'data_img/telegram/telegram_JWygofUEo4FlOWUx.png', 'data_img/telegram/telegram_metamask_usio9.png']
RANDOM_IMGS_LOGO_8 = ['data_img/instagram/instagram_microsoft_espana.png', 'data_img/twitter/twitter_Microsoft_Green.png', 'data_img/twitter/twitter_MicrosoftKorea.png', 'data_img/twitter/twitter_MicrosoftRTweet.png', 'data_img/twitter/twitter_MSEurope.png']
RANDOM_IMGS_LOGO_9 = ['data_img/twitter/twitter_ShopifyEng.png', 'data_img/twitter/twitter_StudioHogan.png', 'data_img/twitter/twitter_TryShopify.png', 'data_img/twitter/twitter_SHOPIFYHelp1.png', 'data_img/twitter/twitter_shopifyturkce.png']

# -------------------------

PATH_DATA_IMG = 'data_img'
PATH_OFFICIAL_IMG = os.path.join(PATH_DATA_IMG, 'official')

# --------------------

name_logo_1 = 'official_netflix'
name_logo_2 = 'official_samsung'
name_logo_3 = 'official_binance'
name_logo_4 = 'official_aboutamazon'
name_logo_5 = 'official_expedia'
name_logo_6 = 'official_walmart'
name_logo_7 = 'official_metamask'
name_logo_8 = 'official_microsoft'
name_logo_9 = 'official_shopify'

# --------------------

DIM_IMGS = (240, 240)

def main():
    set_seed(SEED)

    if not USE_LIST:
        imgs_logo_1 = []
        imgs_logo_2 = []
        imgs_logo_3 = []
        imgs_logo_4 = []
        imgs_logo_5 = []
        imgs_logo_6 = []
        imgs_logo_7 = []
        imgs_logo_8 = []
        imgs_logo_9 = []

        folders_csv = [d for d in os.listdir(PATH_CSV_DATA) if os.path.isdir(os.path.join(PATH_CSV_DATA, d))]

        for folder in tqdm(folders_csv, desc="Folder"):
            thr_folder = os.path.join(PATH_CSV_DATA, folder, f'THR_{THR_VALUE}')
            csv_path = os.path.join(thr_folder, f'thr_{THR_VALUE}_results.csv')

            header, data = read_csv(csv_path)

            col_index_file_name = header.index('file_name')
            col_index_best_official_name = header.index('best_official_name')
            col_index_best_official_score = header.index('best_official_score')

            for riga in tqdm(data, desc="Rows"):
                if float(riga[col_index_best_official_score]) >= SET_THR_VALUE:
                    if riga[col_index_best_official_name] == name_logo_1:
                        imgs_logo_1.append(f'{os.path.join(PATH_DATA_IMG, folder, riga[col_index_file_name])}.png')
                    elif riga[col_index_best_official_name] == name_logo_2:
                        imgs_logo_2.append(f'{os.path.join(PATH_DATA_IMG, folder, riga[col_index_file_name])}.png')
                    elif riga[col_index_best_official_name] == name_logo_3:
                        imgs_logo_3.append(f'{os.path.join(PATH_DATA_IMG, folder, riga[col_index_file_name])}.png')
                    elif riga[col_index_best_official_name] == name_logo_4:
                        imgs_logo_4.append(f'{os.path.join(PATH_DATA_IMG, folder, riga[col_index_file_name])}.png')
                    elif riga[col_index_best_official_name] == name_logo_5:
                        imgs_logo_5.append(f'{os.path.join(PATH_DATA_IMG, folder, riga[col_index_file_name])}.png')
                    elif riga[col_index_best_official_name] == name_logo_6:
                        imgs_logo_6.append(f'{os.path.join(PATH_DATA_IMG, folder, riga[col_index_file_name])}.png')
                    elif riga[col_index_best_official_name] == name_logo_7:
                        imgs_logo_7.append(f'{os.path.join(PATH_DATA_IMG, folder, riga[col_index_file_name])}.png')
                    elif riga[col_index_best_official_name] == name_logo_8:
                        imgs_logo_8.append(f'{os.path.join(PATH_DATA_IMG, folder, riga[col_index_file_name])}.png')
                    elif riga[col_index_best_official_name] == name_logo_9:
                        imgs_logo_9.append(f'{os.path.join(PATH_DATA_IMG, folder, riga[col_index_file_name])}.png')

        random_imgs_logo_1 = random.sample(imgs_logo_1, N_IMGS)
        random_imgs_logo_2 = random.sample(imgs_logo_2, N_IMGS)
        random_imgs_logo_3 = random.sample(imgs_logo_3, N_IMGS)
        random_imgs_logo_4 = random.sample(imgs_logo_4, N_IMGS)
        random_imgs_logo_5 = random.sample(imgs_logo_5, N_IMGS)
        random_imgs_logo_6 = random.sample(imgs_logo_6, N_IMGS)
        random_imgs_logo_7 = random.sample(imgs_logo_7, N_IMGS)
        random_imgs_logo_8 = random.sample(imgs_logo_8, N_IMGS)
        random_imgs_logo_9 = random.sample(imgs_logo_9, N_IMGS)
    else:
        random_imgs_logo_1 = RANDOM_IMGS_LOGO_1
        random_imgs_logo_2 = RANDOM_IMGS_LOGO_2
        random_imgs_logo_3 = RANDOM_IMGS_LOGO_3
        random_imgs_logo_4 = RANDOM_IMGS_LOGO_4
        random_imgs_logo_5 = RANDOM_IMGS_LOGO_5
        random_imgs_logo_6 = RANDOM_IMGS_LOGO_6
        random_imgs_logo_7 = RANDOM_IMGS_LOGO_7
        random_imgs_logo_8 = RANDOM_IMGS_LOGO_8
        random_imgs_logo_9 = RANDOM_IMGS_LOGO_9

    print([elem.split('/')[-1].split('.png')[0] for elem in random_imgs_logo_1])
    print([elem.split('/')[-1].split('.png')[0] for elem in random_imgs_logo_2])
    print([elem.split('/')[-1].split('.png')[0] for elem in random_imgs_logo_3])
    print([elem.split('/')[-1].split('.png')[0] for elem in random_imgs_logo_4])
    print([elem.split('/')[-1].split('.png')[0] for elem in random_imgs_logo_5])
    print([elem.split('/')[-1].split('.png')[0] for elem in random_imgs_logo_6])
    print([elem.split('/')[-1].split('.png')[0] for elem in random_imgs_logo_7])
    print([elem.split('/')[-1].split('.png')[0] for elem in random_imgs_logo_8])
    print([elem.split('/')[-1].split('.png')[0] for elem in random_imgs_logo_9])

    for i, (name_logo, random_imgs_logo) in enumerate(zip([name_logo_1, name_logo_2, name_logo_3, name_logo_4, name_logo_5, name_logo_6, name_logo_7, name_logo_8, name_logo_9], [random_imgs_logo_1, random_imgs_logo_2, random_imgs_logo_3, random_imgs_logo_4, random_imgs_logo_5, random_imgs_logo_6, random_imgs_logo_7, random_imgs_logo_8, random_imgs_logo_9])):

        fig, ax = plt.subplots(1, 3, figsize=(12, 8/4), gridspec_kw={'width_ratios': [1, 0.1, 5]})

        for axis in ax:
            axis.axis('off')

        logo = Image.open(os.path.join(PATH_OFFICIAL_IMG, f'{name_logo}.png')).convert("RGBA")
        logo = logo.resize(DIM_IMGS)

        ax[0].imshow(logo)
        ax[0].text(0.5, 1.05, f"{name_logo.split('official_')[-1]}", fontsize=8.2, ha='center', va='bottom', color='black', transform=ax[0].transAxes)
        rect = Rectangle((-0.5, -0.5), DIM_IMGS[0], DIM_IMGS[0], linewidth=8, edgecolor='red', facecolor='none')
        ax[0].add_patch(rect)

        ax[1].text(0.5, 0.5, 'Spazio vuoto', fontsize=2, ha='center', va='center', color='white')

        images = [Image.open(img).convert("RGBA").resize(DIM_IMGS) for img in random_imgs_logo]

        border_color = (0, 0, 0, 255)
        border_width = 3

        total_width = len(images) * (DIM_IMGS[0] + border_width) - border_width + 2 * border_width
        total_height = DIM_IMGS[0] + 2 * border_width

        separated_imgs = Image.new('RGBA', (total_width, total_height), border_color)

        for k, img in enumerate(images):
            separated_imgs.paste(img, (k * (DIM_IMGS[0] + border_width) + border_width, border_width))

        ax[2].imshow(separated_imgs, aspect='auto')
        accounts_name = [elem.split('/')[-1].split('.png')[0] for elem in random_imgs_logo]
        for k, img in enumerate(random_imgs_logo):
            ax[2].text((k + 0.5) / len(random_imgs_logo), 1.05, f"{'_'.join(accounts_name[k].split('_')[1:])}", fontsize=8.2, ha='center',
                       va='bottom', color='black', transform=ax[2].transAxes)

        plt.tight_layout()

        # plt.show()
        plt.savefig(os.path.join(PATH_CSV_DATA, f"{name_logo.split('official_')[-1].split('.png')[0]}.pdf"), bbox_inches='tight')


if __name__ == '__main__':
    main()
