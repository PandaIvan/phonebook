import re
import pandas as pd


def parse_name(name):
    names = name.split()
    if len(names) == 3:
        return names
    elif len(names) == 2:
        return names + [""]
    return names[:3]


def format_phone(phone):
    pattern = r"(\+?7|8)?\s*\(?(\d{3})\)?[-\s]*(\d{3})[-\s]*(\d{2})[-\s]*(\d{2})(\s*доб\.\s*(\d+))?"
    substitution = r"+7(\2)\3-\4-\5"
    formatted_phone = re.sub(pattern, substitution, phone)

    if "доб." in phone:
        extension = re.search(r"доб\.\s*(\d+)", phone)
        if extension:
            formatted_phone += f" доб.{extension.group(1)}"
    return formatted_phone


def merge_contacts(df):
    grouped_df = df.groupby(["lastname", "firstname"], as_index=False).agg({
        "surname": lambda x: "; ".join(filter(pd.notna, x.unique())),
        "organization": lambda x: "; ".join(filter(pd.notna, x.unique())),
        "position": lambda x: "; ".join(filter(pd.notna, x.unique())),
        "phone": lambda x: "; ".join(filter(pd.notna, x.unique())),
        "email": lambda x: "; ".join(filter(pd.notna, x.unique()))
    })
    return grouped_df


def clean_contacts(file_path, output_path):
    df = pd.read_csv(file_path, header=0,
                     names=["lastname", "firstname", "surname", "organization", "position", "phone", "email"])

    for idx, contact in df.iterrows():
        lastname, firstname, surname = parse_name(
            " ".join([str(contact["lastname"]) if pd.notna(contact["lastname"]) else '',
                      str(contact["firstname"]) if pd.notna(contact["firstname"]) else '',
                      str(contact["surname"]) if pd.notna(contact["surname"]) else '']))
        df.at[idx, "lastname"], df.at[idx, "firstname"], df.at[idx, "surname"] = lastname, firstname, surname

        if pd.notna(contact["phone"]):
            df.at[idx, "phone"] = format_phone(str(contact["phone"]))

    merged_df = merge_contacts(df)

    if merged_df.columns.duplicated().any():
        merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

    merged_df.to_csv(output_path, index=False, encoding='utf-8-sig', header=True)


clean_contacts("phonebook_raw.csv", "cleaned_phonebook.csv")
