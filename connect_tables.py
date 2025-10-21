import pandas as pd
from thefuzz import fuzz
import copy

DIRECTORY = "./exports/"
LINKED = {
    "clinique_sku": [],
    "sephora_sku": [],
    "fuzzy_ratio": [],
    "clinique_name": [],
    "sephora_name": [],
    "sephora_url": [],
    "clinique_url": [],
}
UNLINKED = copy.deepcopy(LINKED)


def add_linked(clinique, sephora_df, sim_index, sim_ratio):
    LINKED["clinique_sku"].append(str(clinique["sku"]))
    LINKED["clinique_name"].append(str(clinique["product_name"]))
    LINKED["clinique_url"].append(str(clinique["url"]))

    LINKED["sephora_sku"].append(str(sephora_df.loc[sim_index, "sku"]))
    LINKED["sephora_url"].append(str(sephora_df.loc[sim_index, "url"]))
    LINKED["sephora_name"].append(str(sephora_df.loc[sim_index, "product_name"]))
    LINKED["fuzzy_ratio"].append(int(sim_ratio))

    return LINKED


def add_unlinked(clinique, sephora_df, sim_index, sim_ratio):
    UNLINKED["clinique_sku"].append(str(clinique["sku"]))
    UNLINKED["clinique_name"].append(str(clinique["product_name"]))
    UNLINKED["clinique_url"].append(str(clinique["url"]))

    UNLINKED["sephora_sku"].append(str(sephora_df.loc[sim_index, "sku"]))
    UNLINKED["sephora_url"].append(str(sephora_df.loc[sim_index, "url"]))
    UNLINKED["sephora_name"].append(str(sephora_df.loc[sim_index, "product_name"]))
    UNLINKED["fuzzy_ratio"].append(int(sim_ratio))

    return UNLINKED


def link(directory=DIRECTORY):
    sephora_df = pd.read_excel(directory + "sephora_rating.xlsx")
    clinique_df = pd.read_excel(directory + "clinique_rating.xlsx")

    for _, clinique in clinique_df.iterrows():
        sim_ratios = []
        sim_partial_ratios = []
        for _, sephora in sephora_df.iterrows():
            # TODO: implement the other level of fuzzy strings since something like Clinique For Men Charcoal Face Wash was not present in linked table because of the first 3 words not being present
            ratio = fuzz.ratio(
                str(clinique["product_name"]).strip().lower(),
                str(sephora["product_name"]).strip().lower(),
            )
            partial_ratio = fuzz.partial_ratio(
                str(clinique["product_name"]).strip().lower(),
                str(sephora["product_name"]).strip().lower(),
            )
            sim_ratios.append(ratio)
            sim_partial_ratios.append(partial_ratio)

        sim_ratio = max(sim_ratios)
        sim_index = sim_ratios.index(sim_ratio)
        if sim_ratio > 72:
            if sephora_df.loc[sim_index, "sku"] in LINKED["sephora_sku"]:
                index = LINKED["sephora_sku"].index(sephora_df.loc[sim_index, "sku"])
                if sim_ratio > LINKED["fuzzy_ratio"][index]:
                    add_linked(clinique, sephora_df, sim_index, sim_ratio)
            elif sephora_df.loc[sim_index, "sku"] not in LINKED["sephora_sku"]:
                add_linked(clinique, sephora_df, sim_index, sim_ratio)
        else:
            # TODO: add additional filtering for partial_ratio
            add_unlinked(clinique, sephora_df, sim_index, sim_ratio)

    df = pd.DataFrame(LINKED)
    df = df.drop_duplicates()
    df_unlinked = pd.DataFrame(UNLINKED)
    df_unlinked = df_unlinked.drop_duplicates()
    df.to_excel(directory + "connection_table.xlsx", index=False)
    df_unlinked.to_excel(directory + "unlinked_table.xlsx", index=False)

    return LINKED, UNLINKED

# run 
link(directory=DIRECTORY)
