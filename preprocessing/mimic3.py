import csv
import numpy as np
import os
import pandas as pd
from tqdm import tqdm

from util import dataframe_from_csv

def read_icustays_table(mimic3_path):
    stays = dataframe_from_csv(os.path.join(mimic3_path, 'ICUSTAYS.csv'))
    stays.INTIME = pd.to_datetime(stays.INTIME)
    stays.OUTTIME = pd.to_datetime(stays.OUTTIME)
    return stays

def read_icd_diagnoses_table(mimic3_path):
    codes = dataframe_from_csv(os.path.join(mimic3_path, 'D_ICD_DIAGNOSES.csv'))
    codes = codes[['ICD9_CODE', 'SHORT_TITLE', 'LONG_TITLE']]
    diagnoses = dataframe_from_csv(os.path.join(mimic3_path, 'DIAGNOSES_ICD.csv'))
    diagnoses = diagnoses.merge(codes, how='inner', left_on='ICD9_CODE', right_on='ICD9_CODE')
    diagnoses[['SUBJECT_ID', 'HADM_ID', 'SEQ_NUM']] = diagnoses[['SUBJECT_ID', 'HADM_ID', 'SEQ_NUM']].astype(int)
    return diagnoses

def filter_diagnoses_on_stays(diagnoses, stays):
    return diagnoses.merge(stays[['SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID']].drop_duplicates(), how='inner',
                           left_on=['SUBJECT_ID', 'HADM_ID'], right_on=['SUBJECT_ID', 'HADM_ID'])

def read_events_table_by_row(mimic3_path, table):
    HBV_HADM = pd.read_csv('HBV_HADM.csv')
    HBV_HADM_list = str(HBV_HADM.HADM_ID.to_list())
    HBV_SUBJECT = pd.read_csv('HBV_SUBJECT.csv')
    HBV_SUBJECT_list = str(HBV_SUBJECT.SUBJECT_ID.to_list())
    #print(HBV_SUBJECT_list)
    item_id = ['772', '1521', '227456',
               '769', '220644',
               '770', '220587',
               '225651',
               '225690',
               '1522',
               '789', '1524', '220603',
               '3747', '1523',
               '791', '1525',
               '227444',
               '811', '3744', '1529', '220621', '225664', '226537',
               '226730',
               '814', '220228',
               '829', '3792', '1535',
               '821', '1532',
               '837', '3803', '1536', '220645', '226534',
               '4375',
               '827', '1534',
               '828', '3789',
               '3793',
               '833',
               '3801',
               '849','1539', '220650',
               '1542',
               '763']
    labitem_id = ['50862', '51025',
                  '50863',
                  '50861',
                  '50878',
                  '50893',
                  '50907',
                  '50902', '51030',
                  '50912',
                  '51114', '51200', '51347', '51368', '51419', '51444',
                  '50809', '50931', '51478',
                  '50811', '51222',
                  '50971',
                  '50813',
                  '51248',
                  '51250',
                  '50960', '51037',
                  '51256',
                  '50970',
                  '51265',
                  '51493',
                  '50883', '50884', '50885',
                  '50976',
                  '51006',
                  '51516',
                  '51464',
                  '50889']
    nb_rows = {'chartevents': 330712484, 'labevents': 27854056, 'outputevents': 4349219}
    reader = csv.DictReader(open(os.path.join(mimic3_path, table.upper() + '.csv'), 'r'))
    for i, row in enumerate(reader):
        if 'ICUSTAY_ID' not in row:
            row['ICUSTAY_ID'] = ''
        if row['HADM_ID'] in HBV_HADM_list or row['SUBJECT_ID'] in HBV_SUBJECT_list:
            if row['ITEMID'] in item_id or row['ITEMID'] in labitem_id:
                yield row, i, nb_rows[table.lower()]

def count_icd_codes(diagnoses, output_path=None):
    codes = diagnoses[['ICD9_CODE', 'SHORT_TITLE', 'LONG_TITLE']].drop_duplicates().set_index('ICD9_CODE')
    codes['COUNT'] = diagnoses.groupby('ICD9_CODE')['ICUSTAY_ID'].count()
    codes.COUNT = codes.COUNT.fillna(0).astype(int)
    codes = codes[codes.COUNT > 0]
    if output_path:
        codes.to_csv(output_path, index_label='ICD9_CODE')
    return codes.sort_values('COUNT', ascending=False).reset_index()

def read_events_table(mimic3_path, table, output_path,
                                              items_to_keep=None, subjects_to_keep=None):
    obs_header = ['SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID', 'CHARTTIME', 'ITEMID', 'VALUE', 'VALUEUOM']
    if items_to_keep is not None:
        items_to_keep = set([str(s) for s in items_to_keep])
    if subjects_to_keep is not None:
        subjects_to_keep = set([str(s) for s in subjects_to_keep])

    class DataStats(object):
        def __init__(self):
            self.curr_subject_id = ''
            self.curr_obs = []

    data_stats = DataStats()

    def write_current_observations():
        dn = os.path.join(output_path)
        fn = os.path.join(dn, 'events.csv')
        if not os.path.exists(fn) or not os.path.isfile(fn):
            f = open(fn, 'w')
            f.write(','.join(obs_header) + '\n')
            f.close()
        w = csv.DictWriter(open(fn, 'a'), fieldnames=obs_header, quoting=csv.QUOTE_MINIMAL)
        w.writerows(data_stats.curr_obs)
        data_stats.curr_obs = []

    nb_rows_dict = {'chartevents': 330712484, 'labevents': 27854056, 'outputevents': 4349219}
    nb_rows = nb_rows_dict[table.lower()]

    for row, row_no, _ in tqdm(read_events_table_by_row(mimic3_path, table), total=nb_rows,
                                                        desc='Processing {} table'.format(table)):

        if (subjects_to_keep is not None) and (row['SUBJECT_ID'] not in subjects_to_keep):
            continue
        if (items_to_keep is not None) and (row['ITEMID'] not in items_to_keep):
            continue

        row_out = {'SUBJECT_ID': row['SUBJECT_ID'],
                   'HADM_ID': row['HADM_ID'],
                   'ICUSTAY_ID': '' if 'ICUSTAY_ID' not in row else row['ICUSTAY_ID'],
                   'CHARTTIME': row['CHARTTIME'],
                   'ITEMID': row['ITEMID'],
                   'VALUE': row['VALUE'],
                   'VALUEUOM': row['VALUEUOM']}
        if data_stats.curr_subject_id != '' and data_stats.curr_subject_id != row['SUBJECT_ID']:
            write_current_observations()
        data_stats.curr_obs.append(row_out)
        data_stats.curr_subject_id = row['SUBJECT_ID']

    if data_stats.curr_subject_id != '':
        write_current_observations()
