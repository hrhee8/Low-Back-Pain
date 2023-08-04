# -*- coding: utf-8 -*-
"""
Created on Wed Nov  3 09:22:59 2022

@author: David Shin and Bardiya Akhbari
@edited: Hyunjoon Rhee

This code loads and cleanes the RPDR text data, runs the first pass of filtering based on ICD and CPT codes
and it then stores them for further processing.

The whole goal is to find the unique EMPI for patients that pass these filters.

NOTE: DO NOT PRESS THE GREEN ARROW BUTTON AT THE TOP!
"""


# Clear workspace variables
from IPython import get_ipython
get_ipython().magic('reset -sf')

import sys
import os
import pickle
import pandas as pd
import re
import datetime
from tqdm import tqdm

import sys, ctypes as ct
import csv

#os.chdir(r'Z:\AI Registries\Projects\Temporal Validation\codes')
#os.getcwd()

#%% Project Specific Variables
from ProjectPaths import *

import RPDR_Processor as rpdrp

import util_rpdr


save_mode = True


#%%
# Tutorial:
# 1) Choose one of the sequences below that correspond to what you would like to do.
# 2) Run the cell above to import the appropriate modules (click anywhere in that cell and then press Ctrl+Enter)
# 3) Enter the values 
# 4) Run the cells in order for the sequence you would like to execute.

# Converting all available txt files that begin with [namestem] into xlsx:
    
# NOTE: DO NOT PRESS THE GREEN ARROW BUTTON AT THE TOP!
# NOTE: DO NOT PRESS THE GREEN ARROW BUTTON AT THE TOP!
# NOTE: DO NOT PRESS THE GREEN ARROW BUTTON AT THE TOP!
# NOTE: DO NOT PRESS THE GREEN ARROW BUTTON AT THE TOP!
# NOTE: DO NOT PRESS THE GREEN ARROW BUTTON AT THE TOP!
# NOTE: DO NOT PRESS THE GREEN ARROW BUTTON AT THE TOP!
# NOTE: DO NOT PRESS THE GREEN ARROW BUTTON AT THE TOP!
# NOTE: DO NOT PRESS THE GREEN ARROW BUTTON AT THE TOP!
# NOTE: DO NOT PRESS THE GREEN ARROW BUTTON AT THE TOP!
# NOTE: DO NOT PRESS THE GREEN ARROW BUTTON AT THE TOP!

# Thank you.
    
# input("You pressed the green arrow button, didn't you? \
#       Please press the red square on the right-hand corner of the console and restart your kernel.")


#%% Criteria on the RPDR Data
check_procedure = True 
check_diagnosis = True #diagnosis for infection

cpt_based_on = 'file' # 'custom' or 'AAOS'

if cpt_based_on == 'AAOS':
    print(1)
    sel_reg = ['SER']
    sel_module = ['Shoulder Arthroplasty']

print(cpt_based_on)




#%% Processing for Infection ---- Go through the lines step-by=step based on the project
# (1) Begin with Procedures.
if save_mode:
    if check_procedure:
        print('Checking procedure criteria...')
        # Load the file:
        name = namecode + '_Prc'
        # procedures = rpdrp.RPDRParser(textfile_path + '/' + name + '.txt')
        print(textfile_path + '/' + name + '.txt')
        
        procedures = pd.read_csv(textfile_path + '/' + name + '.txt', sep='|', encoding='utf8', dtype=str)
        
        # check the column names
        print(procedures.keys())
        
        # check the counts to see how many are CPTs or ICDs or something that you can check
        #procedures['Code_Type'].value_counts()
        
      
        ## Looking at CPT codes only
        proc_cpt = procedures[procedures['Code_Type'] == 'CPT']
        
        proc_cpt['Date'] = pd.to_datetime(proc_cpt['Date'])
        
        
        
#%%
        if cpt_based_on == 'file':
            #LBP only
            #incl_cpts = util_rpdr.get_cpts_fromfile(path_inclusion_crit)
            #incl_cpts_text = '|'.join(incl_cpts.str.strip())
            
            #DJD
            incl_cpts2 = util_rpdr.get_cpts_fromfile(path_inclusion_crit2)
            incl_cpts_text2 = '|'.join(incl_cpts2.str.strip())
            
            
            #DJD only
            #proc_cpt_incl = proc_cpt[(proc_cpt['Date'].dt.year == (2021)) &
            #                         (proc_cpt['Code'].str.contains(incl_cpts_text2, regex=True)) 
            #                         ] # the filtered dataframe that we want to include
            
            proc_cpt_incl = proc_cpt[((proc_cpt['Date'].dt.year == (2021)) | (proc_cpt['Date'].dt.year == (2022))) &
                                     (proc_cpt['Code'].str.contains(incl_cpts_text2, regex=True)) 
                                     ] # the filtered dataframe that we want to include
                # &(proc_cpt['Code'].str.contains(incl_cpts_text2, regex=True))
           
         # save proc cpt incl.
        with pd.HDFStore(h5_filepath) as store:
            store['proc_cpt_incl'] = proc_cpt_incl

print('[INFO] We found {} patients with these procedure codes...'.format(len(proc_cpt_incl['EPIC_PMRN'].unique())))

#%%
print(proc_cpt.head())

#%% (2) Diagnosis.
if save_mode:
    if check_diagnosis:
        print('Checking diagnosis criteria...')
        # Load the file:
        name = namecode + '_Dia'
        diagnoses = rpdrp.RPDRParser(textfile_path + '/' + name + '.txt')
        
        # check the column names
        print(diagnoses.keys())
        
        # check the counts to see how many are CPTs or ICDs or something that you can check
        print(diagnoses['Code_Type'].value_counts())
        #diagnoses['Date'] = pd.to_datetime(proc_cpt['Date'])
        #(diagnoses['Date'].dt.year == 2022) &

#%% (2.1) Diagnosis filter with ICD
        #first filter with LBP
        #project_icd_codes = util_rpdr.get_icds_fromfile(path_inclusion_crit)
        
        #adding secondary filter with DJD
        project_icd_codes2 = util_rpdr.get_icds_fromfile(path_inclusion_crit2)
    
        diag_icd10_criteria = diagnoses[((diagnoses['Code_Type'] == "ICD10") | (diagnoses['Code_Type'] == "ICD9")) &
                                             (diagnoses["Code"].str.contains(project_icd_codes2, regex=True))]
    
        # save diagnoses meeting icd10 criteria
        with pd.HDFStore(h5_filepath) as store:
            store['diag_icd10_criteria'] = diag_icd10_criteria
            

#%%

print('[INFO] We found {} patients with these diagnosis codes...'.format(len(diag_icd10_criteria['EPIC_PMRN'].unique())))
print(diag_icd10_criteria.head())

#%% Merge the tables from EMPIS
if save_mode:
    if check_procedure and check_diagnosis:
        print('Filtering based on timeline of the diagnosis and procedure...')
        # [1] we want to see which procedures are missing correspnding diagnosis
        day_thresh = 90
        # difference between diagnosis date and procedure has to be less than 30 days
        proc_diag = pd.merge(left=proc_cpt_incl, right=diag_icd10_criteria, on='EMPI', how='left', suffixes=('.prc','.dia'))
        
        
        # proc_sorted = proc_diag.sort_values(by='EMPI')
        # proc_diag['proc_diag_datediff'] = (pd.to_datetime(proc_diag["Date"]) - pd.to_datetime(proc_diag["Date.dia"])).dt.days
        
        # filter on date difference
        proc_diag["timediff"] = (pd.to_datetime(proc_diag["Date.dia"], errors='raise') - pd.to_datetime(proc_diag["Date.prc"], errors='raise')).dt.days
        
        proc_diag_Ndays = proc_diag[(proc_diag["timediff"] <= day_thresh) & (proc_diag["timediff"] >= 0)]
        
        met_criteria = proc_diag_Ndays


        # proc_diag_30days = proc_diag[abs(proc_diag['proc_diag_datediff']) <= day_thresh]
        # proc_diag_nomatch_30days = proc_diag[abs(proc_diag['proc_diag_datediff']) >= day_thresh]
        
        
        # # [2] we want to see which diagnosis are missing correspnding procedures
        # # difference between diagnosis date and procedure has to be less than 30 days
        # diag_proc = pd.merge(left=diag_icd10_criteria, right=proc_cpt_incl, on='EMPI', how='left', suffixes=('','.prc'))
        
        # dag_sorted = diag_proc.sort_values(by='EMPI')
        
        # diag_proc['diag_proc_datediff'] = (pd.to_datetime(diag_proc["Date.prc"]) - pd.to_datetime(diag_proc["Date"])).dt.days
        
        
        # # filter on date difference
        # diag_proc_30days = diag_proc[(diag_proc['diag_proc_datediff'] <= day_thresh) & (diag_proc['diag_proc_datediff'] >= -day_thresh)]
        
        # diag_proc_nomatch_30days = diag_proc[(diag_proc['diag_proc_datediff'] >= day_thresh) | (diag_proc['diag_proc_datediff'] <= -day_thresh)]
        
        # met_criteria = proc_diag_30days
    
    elif check_procedure:
        met_criteria = proc_cpt_incl
    
    elif check_diagnosis:
        met_criteria = diag_icd10_criteria
        
    else:
        met_criteria = procedures
        
    
    # See which empi's remain
    criteria_type = pd.DataFrame(pd.Series({'Procedure': check_procedure, 'Diagnosis': check_diagnosis}),columns=['Status'])
    print("Selecting the unique patient EMPIs...")
    empis_met_criteria = pd.Series(pd.unique(met_criteria["EMPI"]))
    print("{} patients exist in this dataset that pass the filters.".format(len(empis_met_criteria)))

    with pd.HDFStore(h5_filepath) as store:
        store['met_criteria'] = met_criteria
        store['empis_met_criteria'] = empis_met_criteria
        store['criteria_type'] = criteria_type
    
else:    
    # load:
    with pd.HDFStore(h5_filepath) as store:
        criteria_type = store['criteria_type'] 
        met_criteria = store['met_criteria']    
        empis_met_criteria = store['empis_met_criteria']
    

#%% Checkpoint
# with pd.HDFStore(h5_filepath) as store:
#     # proc_cpt_incl = store['proc_cpt_incl']
#     # diag_icd10_criteria = store['diag_icd10_criteria']
#     # proc_diag_30days = store['proc_diag_30days']
#     # random_proc_nomatch_50 = store['random_proc_nomatch_50']
#     # random_diag_nomatch_50 = store['random_diag_nomatch_50']
#     met_criteria = store['met_criteria']
#     random_met_criteria_200 = store['random_met_criteria_200']
#     # proc_diag_base = store['proc_diag_base']




#%% Select  filtering and filecodes for reading and exporting the data
# generate filtering lists of EMPIs --- 

print('{} filecodes exist. Select from the list below:'.format(len(filecodes_existing)))
print(filecodes_existing)
['Con', 'Dem', 'Dia', 'Dis', 'Enc', 'Hnp', 'Lab', 'Opn', 'Pat', 'Phy', 'Prc', 'Prg', 'Prv', 'Rad', 'Rdt', 'Rfv']

filterEMPIs = {
    # 'proc50' : pd.unique(random_proc_nomatch_50['EMPI']),
    # 'diag50' : pd.unique(random_diag_nomatch_50['EMPI']),
    # 'procdiag200' : pd.unique(random_procdiag_200['EMPI']),
    # 'procdiag' : pd.unique(proc_diag_30days['EMPI'])
    'met_criteria' : empis_met_criteria.values,
    # 'random_met_criteria_200' : pd.unique(random_met_criteria_200['EMPI'])
    }


## In case you want to manually select the extensions
filecodes_selected = ['Con', 'Dem', 'Dia', 'Dis', 'Enc', 'Hnp', 'Med', 'Mic', 'Mrn', 'Opn', 'Pat', 'Phy', 'Prc', 'Prg', 'Rad', 'Rdt', 'Rfv']
# filecodes_selected_small = ['Mrn']



## Filter keys and codes:
# filterkeys = ['proc50','diag50','procdiag200','procdiag']
filterkeys = ['met_criteria']

# we are concatenating everything based on EMPI, so we do not use those tables that have no EMPI in them
filecodes_woEMPI = ['Let','Mrn','Vis']
filecodes = [filecode for filecode in filecodes_selected if filecode not in filecodes_woEMPI]

# filecodes  = filecodes_existing



#%% In case you want to manually check the files
# name = 'JHS26_2022Z0707_154420_Mrn'
# test = rpdrp.RPDRParser(textfile_path + '/' + name + '.txt')



#%% this code is going to take a long time based on te file sizes
# take filecodes and filter keys (containing EMPI codes), and we go through all combinations
# and we store the filtered results and store it in the h5 file, and it returns a runtime dataframe
runtimes = rpdrp.processSaveRPDRText(textfile_path, namecode, filecodes, filterkeys, filterEMPIs, h5_filepath, save_csv=False)

# this tells you how long it takes to run each specific file
print(runtimes)





































