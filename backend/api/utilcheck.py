import pandas as pd
import re
from sklearn.preprocessing import LabelEncoder

def alreadyreviwed(user_id, review_df):
    reviewed_business_df = review_df[review_df["user_id"] == user_id]
    #reviewed_business_df = reviewed_business_df[["business_id", "review_id", "stars"]]
    return reviewed_business_df


def fusion_attr(key, val):
    attr_full = ""
    key = key.replace("'", "")
    sub_string = re.search('u\'(\w*)\'', val)
    if sub_string:
        attr_full = key + "_" + sub_string.group(1)
    else:
        val = val.replace("'", "")
        attr_full = key + "_" + val
    return attr_full


def flatten_attr(nested_field):
    l = []
    if nested_field:
        for (idx, val) in nested_field.items():
            f_level = idx
            s_level = re.findall(r"{(.+)}", val)
            if s_level:
                for element in s_level[0].split(", "):
                    element_splited = element.split(":")
                    key_element = f_level + "_" + element_splited[0].strip()
                    val_element = element_splited[1].strip()
                    to_add_string = fusion_attr(key_element, val_element)
                    l.append(to_add_string)
            else:
                to_add_string = fusion_attr(f_level, val)
                l.append(to_add_string)
    return ", ".join(l)

def business_details(business_df, review_df):
    business_df = business_df[["business_id", "name", "categories", "address", "city", "state", "latitude", "longitude", "stars", "review_count"]]
    new_df = pd.merge(business_df, review_df, how="inner", on="business_id")
    return new_df

def inti_data():
    data_path = "/home/hongphuc95/notebookteam/dataset/"
    # user_active_df = pd.read_json(data_path + "cleaned/user_active.json", lines=True)
    business_df = pd.read_json(data_path + "business.json", lines=True)
    business_df = business_df.dropna(subset=["categories"])
    #business_df['attributes'] = business_df['attributes'].apply(lambda x: flatten_attr(x))

    review_df = pd.read_json(data_path + "cleaned/review_cleaned_2016_2019_Vegas.json", lines=True)
    missing_cat_business = business_df[business_df.isnull()]["business_id"].tolist()
    business_df = business_df.dropna(subset=["categories"])
    #user_review_df_count = review_df.groupby(["user_id", "business_id"]).size().groupby("user_id").size()
    #user_review_active_df = user_review_df_count[user_review_df_count > 10].reset_index()[["user_id"]]
    #user_active_df = review_df[review_df["user_id"].isin(user_review_active_df["user_id"])]
    #user_active_df = user_active_df.groupby(["user_id", "business_id"], as_index=False).mean()

    return business_df, review_df