import argparse
import yaml

from mimic3 import *

arg = argparse.ArgumentParser(description='argument parser')
arg.add_argument('mimic3_path', type=str, help='mimic3 directory')
arg.add_argument('output_path', type=str, help='output path for data')
arg.add_argument('--event_tables', '-e', type=str, nargs='+', help='select event table',default=['CHARTEVENTS', 'LABEVENTS', 'OUTPUTEVENTS'])
arg.add_argument('--itemids_file', '-i', type=str, help='ITEMID file(csv)')
arg.add_argument('--verbose', '-v', dest='verbose', action='store_true', help='Verbosity in output')
args, _ = arg.parse_known_args()

try:
    os.makedirs(args.output_path)
except:
    pass

stays = read_icustays_table(args.mimic3_path)
if args.verbose:
    print('START:\n\tICUSTAY_IDs: {}\n\tHADM_IDs: {}\n\tSUBJECT_IDs: {}'.format(stays.ICUSTAY_ID.unique().shape[0],
          stays.HADM_ID.unique().shape[0], stays.SUBJECT_ID.unique().shape[0]))

stays.to_csv(os.path.join(args.output_path, 'all_stays.csv'), index=False)
diagnoses = read_icd_diagnoses_table(args.mimic3_path)
diagnoses = filter_diagnoses_on_stays(diagnoses, stays)
diagnoses.to_csv(os.path.join(args.output_path, 'all_diagnoses.csv'), index=False)
count_icd_codes(diagnoses, output_path=os.path.join(args.output_path, 'diagnosis_counts.csv'))

subjects = stays.SUBJECT_ID.unique()

items_to_keep = set(
    [int(itemid) for itemid in dataframe_from_csv(args.itemids_file)['ITEMID'].unique()]) if args.itemids_file else None
for table in args.event_tables:
    read_events_table(args.mimic3_path, table, args.output_path, items_to_keep=items_to_keep,
                                              subjects_to_keep=subjects)