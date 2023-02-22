# -*- coding: utf-8 -*-
"""
Created on Wed Dec 15 11:56:34 2021

Multiple functions that can help you organize the data within the RPDR files

@author: Bardiya and David
"""

import os
import pandas as pd 
from sys import platform



#%% Utilitis
def find_drivestem(print_on=False):
    '''
    This code returns the server drivename for MGH-ORAI.
    
    Returns
    -------
    drivestem : str
        Drivestem for MGH-ORAI.

    '''
    path_root = r'AI Registries/Projects'
    os_mode = ""    
    verification_file = r':/'+path_root+'/General Code/RPDR Data Processing/RPDR_Processor.py'
    
    # checks whether or not the system is windows or mac
    if platform == "win32":
        os_mode = "Windows"
        # todo: read all of the connected mapped network drive letters and determine which one is the MGH-ORAI and adjust the path stem accordingly.
        for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            testpath = letter+verification_file
            if os.path.exists(letter+verification_file):
                basepath = testpath.replace('/RPDR_Processor.py','')
        #orqlpath = 'P:\AAOS\General\Code'
    elif platform == "darwin":
        os_mode = "Mac"
        basepath = '/Volumes/MGH-ORAI/'+path_root+'/General Code/RPDR Data Processing'

    # parse drive stem
    drivestem = basepath.split(path_root)[0]

    if print_on:
        print("base path is: "+basepath)
        print("drivestem is: "+drivestem)

    return drivestem



#%% AAOS Files
def get_cpts_AAOS():
    drivestem = find_drivestem()
    path_to_codes = os.path.join(drivestem,'AAOS','General','Documentation','Processing Tables')
    
    cpt_codes = 'AAOS Registries CPT Inclusion Criteria 04152021'
    cpt_codes_more = 'All_CPTs_KM'
    
    df_cpt_init = pd.read_excel(os.path.join(path_to_codes, cpt_codes+'.xlsx'), dtype=str)
    df_cpt_more = pd.read_excel(os.path.join(path_to_codes, cpt_codes_more+'.xlsx'), dtype=str)
    
    df_cpt_init['CPT'] = df_cpt_init['CPT'].apply(str.strip)
    df_cpt_more['CPT'] = df_cpt_more['CPT'].apply(str.strip)
    df_cpt = pd.merge(df_cpt_init, df_cpt_more, how='left', on='CPT')
    
    return df_cpt


def get_filter_keys_AAOS():
    df_cpt = get_cpts_AAOS()
    return df_cpt[['Registry','Module','Category']].drop_duplicates()
    


def get_cpts_fromAAOS(registry=None, module=None, category=None):
    df_cpt = get_cpts_AAOS()
    if registry != None:
        df_cpt = df_cpt[df_cpt['Registry'].isin(registry)]
    if module != None:
        df_cpt = df_cpt[df_cpt['Module'].isin(module)]
    if category != None:
        df_cpt = df_cpt[df_cpt['Category'].isin(category)]        
    
    return list(df_cpt['CPT'].values)
        


#%% Get CPT and ICD codes from files
def get_cpts_fromfile(filepath=''):

    df_cpt = pd.read_excel(filepath, dtype=str, sheet_name='CPT Codes')

    return df_cpt['CPT Code']       



def get_icds_fromfile(filepath='', icd2=0):
    
        
    if icd2 == 1:
    
        df_icd = pd.read_excel(filepath,dtype=str, sheet_name='ICD Codes2')
        
        return '|'.join(df_icd['ICD Code'].values)
   
    
    else:
        
        df_icd = pd.read_excel(filepath,dtype=str, sheet_name='ICD Codes')


        return '|'.join(df_icd['ICD Code'].values)

        
