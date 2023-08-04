# -*- coding: utf-8 -*-
"""
Created on Thu Dec 16 08:00:18 2021

@author: Bardiya Akhbari, David Shin and Avirath Dodabele
@edited: Hyunjoon Rhee
"""


CLEAR_WORKSPACE = True


#%%
# Clear workspace variables
if CLEAR_WORKSPACE:
    from IPython import get_ipython
    get_ipython().magic('reset -sf')

import sys
import os
import pickle # this is a library that helps you store/save/pickle your dataframe 
import re # this is the regular expression library (for text search)
import pandas as pd
import datetime
# import datefinder
from tqdm import tqdm # to make your for-loops look nicer by showing how long each iteration takes

import sys, ctypes as ct
import csv
import numpy as np
import warnings
warnings.filterwarnings("ignore")

import util_rpdr_cleaner as rpdr_cleaner
pd.set_option('display.max_columns', 500) 

#%% Project Specific Variables
from ProjectPaths import * # it reads all of the paths and names related to the project
import RPDR_Processor as rpdrp # it loads the function that can load the RPDR file
import util_rpdr # loads some functions that can help you clean up the RPDR files


#%% Loading the files and joining the tables
# Loading the files and joining the tables
# loadkeys is the name we have for the subset of patients (comes from: "A1_RPDR_DataOrganizer_FirstPass.py")
loadkeys = ['met_criteria'] # other names: ['proc50','diag50','procdiag200','procdiag']
# the name of the RPDR file we want to read 
filecodes_woEMPI = ['Let','Mrn','Vis'] # 'Vis' was too big for the momery
loadcodes = [filecode for filecode in filecodes_existing if filecode not in filecodes_woEMPI]

# initialize the dataframe that will contain all RPDR files
load_dfs = {}

# loading in the data for a given key and filecodes:
for filecode in tqdm(loadcodes):
    for key in loadkeys:
        with pd.HDFStore(h5_filepath) as store:
            if '/' + key + filecode in store.keys():
                load_dfs[filecode] = store['/'+ key+ filecode]
                
                
with pd.HDFStore(h5_filepath) as store:
    met_criteria = store['met_criteria']



#%% Figure out what is in the RPDR file, and how many encounters have we captures based on our filters
keys_in_rpdr_files = {}
n_encounter = {}
n_patients = {}
for key in load_dfs.keys():
    keys_in_rpdr_files[key]   = load_dfs[key].keys().values
    n_encounter[key] = len(load_dfs[key]['EMPI'])
    n_patients[key]  = len(load_dfs[key]['EMPI'].unique())

keys_in_rpdr_files_df = pd.DataFrame(dict([(k, pd.Series(v)) for k,v in keys_in_rpdr_files.items()]))


print('{} patients exist in this dataset.'.format(n_patients['Dem']))
print(n_patients)


#%% To make data consistent, we want to assign MGH MRN to each patient's EMPI
#### Initialize the table for final output based on patients
df = met_criteria.drop_duplicates(["EMPI"]).reset_index(drop=True)["EMPI"]

### Get MRN File from RPDR to adjust the MRN Values to MGH MRNs
name = namecode + '_Mrn'
mrn_file = rpdrp.RPDRParser(textfile_path + '/' + name + '.txt')
mrn_table = mrn_file[['Enterprise_Master_Patient_Index','MGH_MRN']]
mrn_table.rename(columns={"Enterprise_Master_Patient_Index": "EMPI"},inplace=True)

### Append the MGH-MRN to the Dataframe
df = pd.merge(df, mrn_table, on="EMPI", how="left")

#### To check things, replace the key:
# print(col_df['Dis'])

#%%

#%%  [SPECIFIC TO INFECTION PROJECT]: Check with the old dataset
# Read the old dataset dataset
#path_to_file  = os.path.join('..','Data')
#data_filename = 'TJA_Infection_062221.xlsx'
#xls           = pd.ExcelFile(os.path.join(path_to_file, data_filename))
#df_infected   = pd.read_excel(xls, 'Infected')

# Check the duplicates
#non_unique    = set(df["MGH_MRN"]).intersection(set(df_infected["MGH MRN"]))

#if len(non_unique)>0:
#    raise ValueError("{} duplicate patients exist in the dataset!".format(len(non_unique)))
#else:
#    print('No duplicates in the dataset compared to the small dataset evaluated before...')




#%% (0) Create an init file (in case you want to divide the project's chunk into multiple stages)
met_criteria_grouped = met_criteria.groupby('EMPI')

df_dates = pd.DataFrame()
for empi_key, empi_data in met_criteria_grouped:
    print('Processing {}'.format(empi_key))
    
    patient_data = empi_data.drop_duplicates(["EMPI","Date.prc"])

    df_dates = df_dates.append(patient_data[['EMPI','Date.prc','Date.dia','Code_Type.prc', 'Code.prc','Code_Type.dia', 'Code.dia','Diagnosis_Name','Procedure_Name']])
    
df = pd.merge(df, df_dates, on="EMPI", how="left")

print('{} encounters are recorded for {} patients.'.format(len(df), len(df.EMPI.unique())))

df.to_csv(os.path.join(path_saving_root,'dataframe_init.csv'), index=False)


#%%################# START READING RPDR DATA #################%%#
#%% (1) Demographic - Age, BMI, etc.
dem_grouped = load_dfs['Dem'].groupby('EMPI')

dem_cols = ['Gender_Legal_Sex','Date_of_Birth','Age','Language','Race1','Marital_status','Religion', 'Date_Of_Death']
            #'Is_a_veteran']#,'Zip_code','Country']#,'Vital_status']#,'Date_Of_Death']

dem_list = []
counter = 0 
for empi_key, empi_data in dem_grouped:
    dem_list.append( [empi_key]+empi_data[dem_cols].values[0].tolist() )
    
df_dem = pd.DataFrame(dem_list, columns=['EMPI']+dem_cols)
df = pd.merge(df, df_dem, on="EMPI", how="left")

df['AgeAtPrc'] = np.round((pd.to_datetime(df['Date.prc']) - pd.to_datetime(df['Date_of_Birth'])).dt.days / (365.25))

df.drop(columns=['Date_of_Birth'], inplace = True)



########## At this stage "df" has the "Dem" information to it

#%% (2) Encounter Data
# Encounter Data to Access "Admit Date, Discharge Date, Discharge Disposition and Date"
data_enc = load_dfs['Enc']

# we need these columns to have a sense of the diagnosis list when the patient was admitted
diagnosis_keys = ['Principal_Diagnosis', 'Diagnosis_1', 'Diagnosis_2', 'Diagnosis_3', 'Diagnosis_4',
                  'Diagnosis_5', 'Diagnosis_6', 'Diagnosis_7', 'Diagnosis_8', 'Diagnosis_9', 'Diagnosis_10']


#%%% (2.1) To keep the relevant encounters with patients, we need to filter them based on the Admit/Procedure/Diagnosis/Discharge Dates

### Merge the dataframes -- This will give you the combination of all encounters and patient ids
df_temp = pd.merge(df, data_enc, on="EMPI", how="left")

## Define various time filters:
# [i]   Discharge to Admit Days - This calculates the "Hospital Stay"
dis_admit_days = rpdr_cleaner.calculate_timediff(df_temp['Discharge_Date'], df_temp['Admit_Date'])

# [ii]  Discharge to Procedure Days (This has to be positive or zero)
dis_prc_days   = rpdr_cleaner.calculate_timediff(df_temp['Discharge_Date'], df_temp['Date.prc'])

# [iii] Procedure to Admit Days (This has to be positive or zero)
prc_admit_days = rpdr_cleaner.calculate_timediff(df_temp['Date.prc'], df_temp['Admit_Date'])

# [iv]  Diagnosis to Discharge Days (This has to be positive or zero) --- THIS DOES NOT HAVE TO BE TRUE
dis_dia_days   = rpdr_cleaner.calculate_timediff(df_temp['Discharge_Date'], df_temp['Date.dia'])



## Filter the dataset based on the time criteria of procedure being between Admit & Discharge
data_timefiltered = df_temp[(dis_prc_days.values>=0) # procedure has to be before discharge
                            & (prc_admit_days.values>=0) # procedure date has to be on or after admission date
                            & (dis_admit_days.values<=90) # make sure that we're excluding outlier cases that the patient has been in the hospital for 90 days
                            ].reset_index(drop=True)

# Clean the variables, we do not need these filters anymore
del(dis_admit_days, dis_prc_days, prc_admit_days, dis_dia_days)


#%%% (2.2) Now that our dataset is cleaned based on time, we check patients one-by-one and find the values we care about
#### ----> We care about Discharge_Date, Discharge_Disposition, Admit_Date, and Diagnosis list from ENCOUNTER File
temp_grouped = data_timefiltered.groupby('EMPI')
# Keys that exist in the Encounter dataframe:
#  'EMPI', 'EPIC_PMRN', 'MRN_Type', 'MRN', 'Encounter_number','Encounter_Status', 'Hospital', 'Inpatient_Outpatient', 'Service_Line',
#  'Attending_MD', 'Admit_Date', 'Discharge_Date', 'LOS_Days',
#  'Clinic_Name', 'Admit_Source', 'Discharge_Disposition', 'Payor',
#  'Admitting_Diagnosis', 'Principal_Diagnosis', 'Diagnosis_1',
#  'Diagnosis_2', 'Diagnosis_3', 'Diagnosis_4', 'Diagnosis_5',
#  'Diagnosis_6', 'Diagnosis_7', 'Diagnosis_8', 'Diagnosis_9',
#  'Diagnosis_10', 'DRG', 'Patient_Type', 'Referrer_Discipline'

df_encounters = pd.DataFrame()
for empi_key, empi_data in temp_grouped:        
    print('----\nProcessing {}'.format(empi_key))
    # FOR DEBUGGING REASONS (uncomment if you want to stop the code at a certain patient's id)
    # if empi_key == '100059763':
    #         break

    # create a dataframe for the patient encounters
    df_enc = empi_data[df.keys().tolist() + ['Admit_Date','Discharge_Date',
                                             'Discharge_Disposition','Encounter_Status'] + diagnosis_keys]
    
    # sort the dataframe based on the admit data in order to have an organized way of processing the dataset
    df_enc.sort_values(by=['Admit_Date'],inplace=True)
    
    # to ensure we have a discharge disposition (and probably the last encounter with the patient), we need to replace empty cells
    df_enc.Encounter_Status.replace('', np.nan, inplace=True)
    df_enc.Discharge_Disposition.replace('', np.nan, inplace=True)
    
    # dropping the duplicates for the same/similar encounters
    df_enc.drop_duplicates(["EMPI","Admit_Date","Discharge_Date","Discharge_Disposition"], inplace=True)
    
    # dropping those encounters that are cancelled
    df_enc.drop(df_enc[df_enc['Encounter_Status'] == 'Cancelled'].index, inplace = True)
    
    # dropping the following that is more restrict    
    df_enc.drop_duplicates(["EMPI","Admit_Date","Discharge_Disposition"], inplace=True)


    if sum(~df_enc.Discharge_Disposition.isna()) > 0:
        print(df_enc[["EMPI","Admit_Date","Discharge_Disposition","Encounter_Status"]])
        df_enc.dropna(subset=['Discharge_Disposition'], inplace=True)
        print('Cleaned to:')
        print(df_enc[["EMPI","Admit_Date","Discharge_Disposition","Encounter_Status"]])

    df_enc.drop(columns=['Encounter_Status'], inplace = True)        
    
    df_enc['diagnosis_list'] = df_enc[diagnosis_keys].apply(lambda x: ', '.join(x).strip(', '), axis = 1)
    df_enc.drop(columns=diagnosis_keys, inplace = True)        


    df_encounters = df_encounters.append(df_enc)
    

# This will be the completed dataframe
df_encounters.reset_index(drop=True, inplace=True)


####### ----------------------Code Review: 01/27/2022 by Avi, David, and Bardiya
#%% (3) Contanct Information Data to access the Social Determinents
data_con = load_dfs['Con']
data_con.drop(columns=['EPIC_PMRN', 'MRN_Type', 'MRN', 'Last_Name', 'First_Name',\
                       'Middle_Name', 'Research_Invitations', 'Day_Phone', 'SSN', 'VIP',\
                       'Previous_Name', 'Patient_ID_List', 'Primary_Care_Physician',\
                       'Resident_Primary_Care_Physician', 'Address1', 'Address2'], inplace = True)
# 'EMPI', 'Address1', 'City', 
# 'State', 'Zip', 'Country', 'Home_Phone', 
# 'Insurance_1', 'Insurance_2', 'Insurance_3', 

# keeping the first 3-digits of the phone number
data_con['Phone_Code'] = data_con['Home_Phone'].apply(lambda x: x.strip()[:3])
data_con.drop(columns=['Home_Phone'], inplace = True)    

# clean up the insurance names and stripping the insurance number etc.
data_con['Insurance_1'] = data_con['Insurance_1'].apply(lambda x: x.split(',')[0].strip(', '))
data_con['Insurance_2'] = data_con['Insurance_2'].apply(lambda x: x.split(',')[0].strip(', '))
data_con['Insurance_3'] = data_con['Insurance_3'].apply(lambda x: x.split(',')[0].strip(', '))

                       
### Merge them to evaluate the time
df_sodh = pd.merge(df_encounters, data_con, on="EMPI", how="left")

# To save memory
del(data_con)


#%% (4) Physical Data to Access BMI, Height, Weight, Smoking Habits, Alcohol Use, and Sexual Activity
data_phy = load_dfs['Phy']
# 'EMPI', 'EPIC_PMRN', 'MRN_Type', 'MRN', 'Date', 'Concept_Name',
# 'Code_Type', 'Code', 'Result', 'Units', 'Provider', 'Clinic',
# 'Hospital', 'Inpatient_Outpatient', 'Encounter_number'

### Merge them to evaluate the time
df_temp = pd.merge(df_encounters, data_phy, on="EMPI", how="left")

# Get the Discharge to Procedure Days (This has to be positive or zero)
dis_date_days   = rpdr_cleaner.calculate_timediff(df_temp['Discharge_Date'], df_temp['Date'])
# Get the Procedure to Admit Days (This has to be positive or zero)
date_admit_days = rpdr_cleaner.calculate_timediff(df_temp['Date'], df_temp['Admit_Date'])

# Filter the dataset based on the time criteria of procedure being between Admit & Discharge
before_admit_days_threshold = 90
data_timefiltered = df_temp[(dis_date_days.values>=-before_admit_days_threshold)
                            & (date_admit_days.values>=-before_admit_days_threshold)
                            ].reset_index(drop=True)
print('Checking {} days before/after admitting the patients.'.format(before_admit_days_threshold))

# To save memory
del(dis_date_days, date_admit_days)

#%%% (4.1) Get the concepts that are writtent in the physical data
temp_grouped = data_timefiltered.groupby('EMPI')
concept_names = set()
for empi_key, empi_data in temp_grouped:    
    for concept in empi_data['Concept_Name'].unique():
        concept_names.add(concept)
        
concept_names = pd.Series(sorted(concept_names))

print(concept_names)

#Get values for each concept and store them in dictionary
concept_dict = {}

concept_dict['BMI']    = concept_names[concept_names.str.contains('BMI', case=False)].values

concept_dict['Height'] = concept_names[concept_names.str.contains('Height', case=False)].values

concept_dict['Weight'] = concept_names[concept_names.str.contains('Weight', case=False)].values

concept_dict['Alcohol_Use'] = concept_names[concept_names.str.contains('Alcohol Use', case=False)].values

concept_dict['Smoking_Habit'] = concept_names[concept_names.str.contains('Tobacco Use', case=False, regex =True)].values


#%%% (4.2) Get BMI, Height, Weight, Smoking, Alcohol, and Sexual Activity
temp_grouped = data_timefiltered.groupby('EMPI')
df_physicals = pd.DataFrame()
for empi_key, empi_data in temp_grouped:
    print('Processing {}'.format(empi_key))
    data_physicals = pd.DataFrame() ## just initialize
    # if empi_key == '100073091':
    #       break
    
    ### Get the concepts
    data_bmi    = rpdr_cleaner.get_concept_results_based(empi_data, concept_name = 'BMI', concept_dict=concept_dict, with_unit = False)
    data_height = rpdr_cleaner.get_concept_results_based(empi_data, concept_name = 'Height', concept_dict=concept_dict, with_unit = True)
    data_weight = rpdr_cleaner.get_concept_results_based(empi_data, concept_name = 'Weight', concept_dict=concept_dict, with_unit = True)
    
    data_smoking            = rpdr_cleaner.get_concept_category(empi_data, concept_name = 'Smoking_Habit', concept_dict=concept_dict)
    data_alcohol            = rpdr_cleaner.get_concept_category(empi_data, concept_name = 'Alcohol_Use', concept_dict=concept_dict)
    

    ### Merging the BMI, Height and Weight based on the Date beacuse they're taken together
    data_physicals = pd.merge(data_bmi, data_height, on = "Date", how="inner", suffixes=('','_Height'))
    data_physicals = pd.merge(data_physicals, data_weight, on = "Date", how="inner", suffixes=('','_Weight'))    
    
    ## Find the one closest to the procedure and use those as patient's information
    data_physicals['diff_date'] = abs(pd.to_datetime(data_physicals['Date']) - pd.to_datetime(empi_data['Date.prc'].unique()[0])).dt.days
    data_physicals.sort_values(by='diff_date', inplace=True)

    ## Get the closes timepoint and store it
    data_physicals = data_physicals.head(1)    
    data_others = pd.DataFrame.from_dict({'EMPI': [empi_key],
                             'Smoking': [data_smoking],
                             'Alcohol_Use': [data_alcohol]})
        
    # Make a dataframe for the subject
    df_subject = pd.merge(data_physicals, data_others, on = "EMPI", how="inner")

    # Append to the final dictionary
    df_physicals = df_physicals.append(df_subject)


df_physicals.drop(columns=['EMPI_Weight','EMPI_Height','diff_date'], inplace = True)    
df_physicals.rename(columns={'Date':'Physical_Date',
                             'Result':'BMI',
                             'Result_Weight': 'Weight',
                             'Result_Height' : 'Height'}, inplace=True)
df_physicals.reset_index(drop=True, inplace=True)


# To save memory
del(data_phy, data_timefiltered, df_temp)



#%% Save Data
df_final = pd.merge(df_sodh, df_physicals, on = "EMPI", how="inner")


SAVE = True
if SAVE:
    df_final.to_csv(os.path.join(path_saving_root,'CleanedData_DemEncPhyCon_MM.csv'), index=False)





#%%%

print(len(df_final))



