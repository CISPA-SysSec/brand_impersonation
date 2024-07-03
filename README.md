##  The Imitation Game: Exploring Brand Impersonation Attacks on Social Media Platforms

## Research and Framework Summary
In this research, we analyze the top 10,000 Tranco ranked domains for impersonated social media profiles on four major platforms: X, Instagram, Telegram, and YouTube. This documentation provides a foundation to help future researchers and security communities expand this framework to detect and proactively mitigate brand impersonation attacks. The framework requires private API keys, an in-house database installation, and several other dependencies listed below. It is not a plug-and-play solution.

## Source Code 

We have three main component as part of the artifacts summary. Below we provide further details.

### Data Collection, Squatting Analysis and Disclosure

The first component gathers data from various social platforms based on the top 10K domains, identifying impersonated profiles using squatting techniques. These squatted social media profiles are then analyzed based on their creation and engagement methods. Finally, we disclose our findings to the affected entities. Detailed information about these data points can be found in the README section of the ```code/accounts_collection_and_analysis``` directory. For more details, please refer to this [link](https://github.com/CISPA-SysSec/brand_impersonation/code/accounts_collection_and_analysis/README.md).

### Clustering and Image Analysis

We cluster posts to identify different categories of attacks carried out by impersonating profiles. Additionally, we analyze the use of profile pictures to detect impersonation through image use. For more detailed technical information about post and image clustering, refer to the README section of the ```code/ml_posts_and_image_analysis``` directory. For further details, please refer to this [link](https://github.com/CISPA-SysSec/brand_impersonation/code/ml_posts_and_image_analysis/README.md).

### Experiment

We perform proactive detection experiment to find the squatting profiles targeting the top 10 brands. We provide the detail of experiment directory. 

## Citation 

```
@inproceedings{acharya_brand_impersonation_2024,
  author = {Acharya, Bhupendra and Lazzaro, Dario and López-Morales, Efrén and Oest, Adam and Saad, Muhammad and Emanuele Cinà, Antonio and Schönherr, Lea and Holz, Thorsten},
  title = {{The Imitation Game: Exploring Brand Impersonation Attacks on Social Media Platforms}},
  booktitle="USENIX Security",
  year={2024}
 }
```

## Questions and Collaborations

We welcome any questions or discussions to further explore similar attacks on social media. Please feel free to reach out via ```bhupendra.acharya@cispa.de``` or reach out via personal [website](https://bhupendraacharya.com). 