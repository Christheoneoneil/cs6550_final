import pandas as pd
import os
pd.set_option("display.max_columns", 500)
pd.set_option("display.max_rows", 500)
def read_parse(path:str, remap_dict:dict)->pd.DataFrame:
    """ read and parse relevant columns from scraped data

    Aargs:
        path (str): file path
        parse_dict (dict): dictionary for reading in and 
        renaming relevant column names

    Returns:
        df (pd.DataFrame): read in and parsed data
    """

    df = pd.read_csv(path)
    df = df[list(remap_dict.keys())]
    df = df.rename(columns=remap_dict)
    return df


def clean_text_col(df:pd.DataFrame, str_col:str)->pd.DataFrame:
    """Cleans text column by strpping special characters and white space

    Args:
        df (pd.DataFrame): Pandas data frame that contains string column
        str_col (str): string column for cleaning
    
    Returns:
    df (pd.DataFrame): data frame with cleaned column
    """

    df = df.copy()
    df.loc[:, str_col] = df.loc[:, str_col].str.replace("\n", "")
    df.loc[:, str_col] = df.loc[:, str_col].str.strip()
    return df


def get_topics(df:pd.DataFrame, feature_col:str):
    """using bertopic get topics from feature column 

    Args:
        df: (pd.DataFrame): Pandas data frame with free text column for topic analysis
        feature_col (str): column that contains free text
    
    Returns:
        None
    """
    from bertopic import BERTopic
    from umap import UMAP
    from bertopic.representation import KeyBERTInspired
    umap = UMAP(n_neighbors=15,
            n_components=5,
            min_dist=0.0,
            metric='cosine',
            low_memory=False,
            random_state=1337) 
    representation_model = KeyBERTInspired(random_state=1)
    topic_model = BERTopic(representation_model=representation_model, umap_model=umap)
    topics, probs = topic_model.fit_transform(df[feature_col])
    print(topic_model.get_topic_info())
   

titles_files_dir = "reddit_scraped_data/titles"
title_file_list = os.listdir(titles_files_dir)
title_file_list = [os.path.join(titles_files_dir, file) for file in title_file_list]

title_remap_cols = [{"Content2": "post_title", "Content5": "comment_count"},
                    {"Content2": "post_title", "Content4": "comment_count"},
                    {"Content2": "post_title", "Content5": "comment_count"}]
title_data_list = [read_parse(path=file, remap_dict=remap_cols)[:100] for file,remap_cols in zip(title_file_list, title_remap_cols)]
clean_title_data = [clean_text_col(df=data, str_col="post_title") for data in title_data_list]
title_data = pd.concat(clean_title_data, axis=0, ignore_index=True)

comment_files = ["reddit_scraped_data/comments/chatgpt_privacy_commentsban.csv",
         "reddit_scraped_data/comments/privacy comments.csv",
         "reddit_scraped_data/comments/openai ban.csv"]
str_cols = {"py0":"Text", "Field1":"Text", "Text":"Text"}

comemnts_df_list = [clean_text_col(pd.DataFrame(pd.read_csv(file)[col]), 
                                 str_col=col).rename(columns={col:remap}) for file, col, remap in zip(comment_files, 
                                                                          str_cols.keys(), str_cols.values())]
comments_data = pd.concat(comemnts_df_list, axis=0, ignore_index=True)

# get_topics(df=title_data, feature_col="post_title")
# get_topics(df=comments_data, feature_col="Text")

print(title_data)
title_data["comment_count"] = title_data["comment_count"].apply(lambda x: int(x))

print(title_data.sort_values(by="comment_count"))