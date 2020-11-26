import os
import pandas as pd
import uuid
import shutil
import hashlib
import json

source_file = "Metadata_All.xlsx"
base_dir = os.path.dirname(__file__)
result_dir = os.path.join(base_dir, 'results')

shutil.rmtree(result_dir, ignore_errors = True)

def create_if_not_exist(dir):
    if not os.path.isdir(dir):
        os.mkdir(dir)


def generate_uuid4():
    return str(uuid.uuid4())

sheet_names = list(pd.read_excel(source_file, None).keys())
create_if_not_exist(result_dir)
print(sheet_names)
sheet_names.sort()

packets = dict()
source_id_str = "source_id"
artifact_id_str = "artifact_id"
similarity_packet_str = "simlarity_packet"
unique_packet_str = "unique_packet"
similarity_group_str = "simlarity_group"
unique_group_str = "unique_group"
associate_group_str = "associate_group"
uassociate_group_str = "unique_associate_group"
similarity_packet_prefix = "SP"
unique_packet_prefix = "UP"
similarity_group_prefix = "SG"
unique_group_prefix = "UG"
associate_group_prefix = "AG"
uassociate_group_prefix = "UAG"

all_columns = []

# Ignore columns
pk_columns = [source_id_str, artifact_id_str, "File:Directory",
               "ExifTool:ExifToolVersion", "File:FilePermissions",
                "File:FileInodeChangeDate", "SourceFile"]

print("Calculating the similarity Packets and Unique Packtets ")
for sheet_name in sheet_names:
    print("+-+" * 50)
    print("===> Reading ---->  ", sheet_name)
    unique_packet_value = 1
    similarity_packet_value = 1
    unique_group_value = 1
    similarity_group_value = 1
    packets[sheet_name] = dict()
    packets[sheet_name][similarity_packet_str] = dict()
    sp = packets[sheet_name][similarity_packet_str]
    packets[sheet_name][unique_packet_str] = dict()
    up = packets[sheet_name][unique_packet_str]
    packets[sheet_name][artifact_id_str] = dict()
    af = packets[sheet_name][artifact_id_str]
    packets[sheet_name][similarity_group_str] = dict()
    sg = packets[sheet_name][similarity_group_str]
    packets[sheet_name][unique_group_str] = dict()
    ug = packets[sheet_name][unique_group_str]
    source_dir = os.path.join(result_dir, sheet_name)
    create_if_not_exist(source_dir)

    df = pd.read_excel(source_file, sheet_name)
    df.fillna("", inplace=True)
    columns = list(df.columns)
    all_columns.append(columns)
    sp_list = []
    up_list = []
    for column in columns:
        print("===> Analyzing column ---->  ", column)
        if column in pk_columns:
            continue
        unique_col_values = [_ for _ in df[column].unique().tolist() if str(_).strip()]

        for _uniq_value in unique_col_values:
            if not _uniq_value:
                continue
            # _uniq_value_hash = hashlib.md5(str(_uniq_value).encode()).hexdigest()
            tdf = df[df[column] == _uniq_value][[source_id_str, artifact_id_str, column]]
            tdf = tdf.drop_duplicates()
            length  = tdf.shape[0]
            artifact_id_list = tdf[artifact_id_str].tolist()
            artifact_id_list.sort()
            # if len(artifact_id_list) > 50:
            #     print("       >>>>>>        Culprit unique value - ", _uniq_value,  len(artifact_id_list), column)
            #     continue
            if length > 1:
                sp_list.append("_".join(artifact_id_list))
            else:
                up_list.append("".join(artifact_id_list))

    def af_list(a_id, packet_idx):
        if a_id not in af.keys():
            af[a_id] = list()
        af[a_id].append(packet_idx)

    #  Similarity Packet Classification
    for _list in list(set(sp_list)):
        if not len(_list):
            continue
        packet_idx = similarity_packet_prefix+str(similarity_packet_value)
        sp[packet_idx] = _list.split("_")
        similarity_packet_value += 1
        for _id in _list.split("_"):
            af_list(_id, packet_idx)

    #  Unique Packet Classification
    for _list in up_list:
        if not len(_list):
            continue
        packet_idx = unique_packet_prefix+str(unique_packet_value)
        up[packet_idx] = _list
        unique_packet_value += 1
        af_list(_list, packet_idx)

    # Similarity Group Classification
    print('===> Running similarity/unique group ')
    similarity_tmp = list()
    unique_tmp = list()
    for a_id in af.keys():
        tmp =  list()
        tmp_uniq = list()
        # print("===> Running Artifact Id -- ", a_id)
        for group in af[a_id]:
            if group.startswith("SP"):
                tmp.extend(sp[group])
            if group.startswith("UP"):
                tmp_uniq.append(group)
        sg_list = list(set(tmp))
        sg_list.sort()
        sg_list_str = "_".join(sg_list)
        if not sg_list_str:
            continue
        similarity_tmp.append(sg_list_str)
        unique_tmp.append(tmp_uniq)

    for similarity_str in list(set(similarity_tmp)):
        if not similarity_str:
            continue
        sg[similarity_group_prefix+str(similarity_group_value)] = similarity_str.split("_")
        similarity_group_value += 1

    for uniq_lst in unique_tmp:
        uniq_lst.sort()
        ug[unique_group_prefix+str(unique_group_value)] = uniq_lst
        unique_group_value += 1

    print("Total Similarity Packets - ", similarity_packet_value -1)
    print("Total Unique Packets - ", unique_packet_value -1)
    print("Total Similarity Group - ", similarity_group_value -1)
    print("Total Unique Group - ", unique_group_value -1)
    sp_file = os.path.join(source_dir, "{}_{}.json".format(sheet_name, "_meta_analysis"))
    json.dump(packets[sheet_name], open(sp_file,'w'), indent = 4)


print("+-+" * 50)
print("Calculating Associate Grouping ")
dfs = [pd.read_excel(source_file, sheet_name) for sheet_name in sheet_names]
df = pd.concat(dfs, axis=0, ignore_index=True)
# print(df.shape)

common_columns =  list(set(set(all_columns[0]).intersection(*all_columns[1:])))
df =df[common_columns]
df.fillna("", inplace=True)

sgdf_list = list()
ugdf_list = list()
# create dataframes for SimilarityGroup SG (all)
for sheet_name in sheet_names:
    sg = packets[sheet_name][similarity_group_str]
    print(sheet_name, "SimilarityGroup", len(sg.keys()), "UniqueGroup", len(ug.keys()))
    for key in sg.keys():
        _df = df[df[artifact_id_str].isin(sg[key])]
        _df = _df.drop_duplicates()
        sgdf_list.append(_df)

    ug = packets[sheet_name][unique_group_str]
    up = packets[sheet_name][unique_packet_str]
    for key in ug.keys():
        uniq_lst = list(set([up[_key] for _key in ug[key] if _key.strip()]))
        _df = df[df[artifact_id_str].isin(uniq_lst)]
        _df = _df.drop_duplicates()
        ugdf_list.append(_df)

# search in other columns except X column of SG dataframes
def search_dataframe(df_list, search_value, skip_column=[]):
    match_artificat_ids = []
    _columns = [x for x in common_columns if x not in pk_columns + skip_column]
    for _df in df_list:
        _a_ids = _df[artifact_id_str].unique().tolist()
        _df = _df[_columns]
        _df = _df[_df.eq(search_value).any(1)]
        length = _df.shape[0]
        if length >= 1:
            # print(search_value, skip_column)
            match_artificat_ids.extend(_a_ids)
    return match_artificat_ids

# Association Group
ag_list = list()
associate_group_value = 1
packets[associate_group_str] = dict()
ag = packets[associate_group_str]
# Unique Association Group
uag_list = list()
uassociate_group_value = 1
packets[uassociate_group_str] = dict()
uag = packets[uassociate_group_str]

common_columns.sort()
# Take unique value from X column
for col in common_columns:
    print("===> Analyzing associate column ---->  ", col)
    if col in pk_columns:
        continue
    unique_values = [_ for _ in df[col].unique().tolist() if _ and str(_).strip()]
    if not unique_values:
        continue
    for unique_value in unique_values:
        a_ids = search_dataframe(sgdf_list, search_value=unique_value, skip_column=[col])
        if len(a_ids):
            a_ids =  list(set(a_ids))
            a_ids.sort()
            ag_list.append("_".join(a_ids))

        a_ids = search_dataframe(ugdf_list, search_value=unique_value, skip_column=[col])
        if len(a_ids):
            a_ids =  list(set(a_ids))
            a_ids.sort()
            uag_list.append("_".join(a_ids))

# Group the SG -> AG columns
for _list in list(set(ag_list)):
    if not len(_list):
        continue
    packet_idx = associate_group_prefix+str(associate_group_value)
    ag[packet_idx] = _list.split("_")
    associate_group_value += 1

# Group the SG -> UAG columns
for _list in list(set(uag_list)):
    if not len(_list):
        continue
    packet_idx = uassociate_group_prefix+str(uassociate_group_value)
    uag[packet_idx] = _list.split("_")
    uassociate_group_value += 1

print("Total Associate Group - ", associate_group_value -1)
print("Total Unique Associate Group - ", uassociate_group_value -1)
ag_file = os.path.join(result_dir, "{}.json".format("All_Meta_Analysis"))
json.dump(packets, open(ag_file,'w'), indent = 4)
