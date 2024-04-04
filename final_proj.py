import pandas as pd
import os


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


def clean_title_col(df:pd.DataFrame, str_col:str)->pd.DataFrame:
    """Cleans title column by strpping special characters and white space

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

    
titles_files_dir = "reddit_scraped_data/titles"
title_file_list = os.listdir(titles_files_dir)
title_file_list = [os.path.join(titles_files_dir, file) for file in title_file_list]
title_remap_cols = {"Content2": "post_title", "Content5": "comment_count"}
title_data_list = [read_parse(path=file, remap_dict=title_remap_cols)[:100] for file in title_file_list]
clean_title_data = [clean_title_col(df=data, str_col="post_title") for data in title_data_list]
title_data = pd.concat(clean_title_data, axis=0, ignore_index=True)

files = ["reddit_scraped_data/comments/chatgpt_privacy_commentsban.csv",
         "reddit_scraped_data/comments/privacy comments.csv",
         "reddit_scraped_data/comments/openai ban.csv"]
str_cols = {"py0":"Text", "Field1":"Text", "Text":"Text"}

comemnts_df_list = [clean_title_col(pd.DataFrame(pd.read_csv(file)[col]), 
                                 str_col=col).rename(columns={col:remap}) for file, col, remap in zip(files, 
                                                                          str_cols.keys(), str_cols.values())]
comments_data = pd.concat(comemnts_df_list, axis=0, ignore_index=True)
print(title_data)
print(comments_data)
