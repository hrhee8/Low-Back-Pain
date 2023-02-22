# -*- coding: utf-8 -*-
"""
Created on Tue Jan 11 10:58:38 2022

This script is intended to associate relevant diagnosis records with each infection entry in order to
assess the comorbidity burden at time of the index surgery pre-infection.


@author: David and Bardiya 
"""

##################### DO NOT CHANGE
# Clear workspace variables
from IPython import get_ipython
get_ipython().magic('reset -sf')

import pandas as pd
import sys
import os
import pickle
import pandas as pd
import re
import datetime
from tqdm import tqdm
import numpy as np
import warnings
warnings.filterwarnings("ignore")

from ProjectPaths import *
import RPDR_Processor as rpdrp
import util_rpdr
##################### END



#%% ADD YOUR LIBRARIES HERE



##################### DO NOT CHANGE
#%% Loading the dataframe with EMPI, MRN, Prognosis & Diagnosis Date and Names
print(path_saving_root)
df = pd.read_csv(os.path.join(path_saving_root,'dataframe_init.csv'), dtype=dtype_dict)  
# Keys:'EMPI', 'MGH_MRN', 'Date.prc', 'Date.dia', 'Diagnosis_Name','Procedure_Name


### Loading the files and joining the tables
loadkeys = ['met_criteria']#['proc50','diag50','procdiag200','procdiag']
filecodes_woEMPI = ['Let','Mrn','Vis'] # 'Vis' was too big for the momery
loadcodes = [filecode for filecode in filecodes_existing if filecode not in filecodes_woEMPI]


load_dfs = {}
loadcodes = ['Dia']
# loading in the data for a given key and filecodes:
for filecode in tqdm(loadcodes):
    for key in loadkeys:
        with pd.HDFStore(h5_filepath) as store:
            if '/' + key + filecode in store.keys():
                load_dfs[filecode] = store['/'+ key+ filecode]



##################### END
#%% Processing Diagnosis Data:

# loading in the full file of diagnosis records for the patients of interest
dias = load_dfs['Dia']
base_dataframe = df

print(base_dataframe.head())

#%%
duplicate_columns = ["EMPI","Date.prc"]

# renaming any columns pertaining to diagnosis data to include ".pastdia" suffix to distinguish infection diagnoses
# in our starting dataset from the diagnoses we will be considering as potential comorbidities, etc.
dias = dias.rename(columns={old:new for (old,new) in [(old,old+'.pastdia') for old in list(dias.columns)] if (old != "EMPI")})
print(dias.keys())

#%%
# merge onto set of patient empis and dates
df_dias = pd.merge(base_dataframe,dias,on="EMPI",how="left",suffixes=("",".pastdia"))

#%%
#print(df_dias.keys())
#print(np.shape(df_dias))
print(type(df_dias['EMPI'][0]))


#%% creating 

#%% DJD
#DJD
project_icd_codes = util_rpdr.get_icds_fromfile(path_inclusion_crit)
#LBP
project_icd_codes2 = util_rpdr.get_icds_fromfile(path_inclusion_crit2)

for i in range(len(df_dias-1)):
    int DJD = 0
    int LBP = 0
    EMPI_num = df_dias['EMPI'][i]
    EMPI_init = EMPI_num
    
    if (df_dias['EMPI'][i] == EMPI_init):
        if 
        int(df_dias['Code.dia'][i]) == 
            (proc_cpt['Code'].str.contains(incl_cpts_text, regex=True)

#%%


SAVE = True
if SAVE:
    df_final.to_csv(os.path.join(path_saving_root,'new_dataframe_init.csv'), index=False)

#%%
# determine the difference in days to the diagnosis date from the initial procedure or from the infection diagnosis dates
#df_dias["Datediff_Dia-Prc"] = (pd.to_datetime(df_dias["Date.pastdia"]) - pd.to_datetime(df_dias["Date.prc"])).dt.days
df_dias["Datediff_Dia-Prc"] = (pd.to_datetime(df_dias["Date.pastdia"]) - pd.to_datetime(df_dias["Date.prc"])).dt.days

# NOTE: decided to use the procedure date as the point of reference for time comparisons with potential comorbidity diagnosis data.
# filter out everything that is from after the initial procedure:
df_dias = df_dias[df_dias["Datediff_Dia-Prc"] < 0]

# Check proportion of icd10 to icd9 codes to see if we should assess both
df_dias["Code_Type.pastdia"].value_counts()
# NOTE: ICD10 is 135090 and ICD9 is 312. Let's just process the icd10s for now.
# NOTE: Running the code again, it seems the balance between icd10 and icd9 is 128933 vs 110480. I think we should assess both.

df_dias_icd10 = df_dias[df_dias["Code_Type.pastdia"] == "ICD10"]
df_dias_icd9 = df_dias[df_dias["Code_Type.pastdia"] == "ICD9"]


#%% Diagnosis Categorization

# now we are going to label each of these by category using appropriate ICD9 or ICD10 codes:
    
#%% Comorbidities:
    
## Dictionary of Comorbidity Definitions:
    
Acute_MI_ICD9 = ["410", "412"]
Acute_MI_ICD10 = ["I21", "I22", "I252"] # david look at this later to make sure what I252 is supposed to be.

CHF_ICD9 = ["428"]
CHF_ICD10 = ["I50"]

Peripheral_vascular_disease_ICD9 = ["441", "443.9", "785.4", "V43.4"]
Peripheral_vascular_disease_ICD10= ["I71", "I79.0", "I73.9", "R02", "Z95.8", "Z95.9"]

CVA_ICD9 = ["430", "431", "432", "433", "434", "435", "436", "438"]
CVA_ICD10 = ["I60", "I61", "I62", "I63", "I65", "I66", "G45.0", "G45.1", "G45.2", "G45.8", "G45.9", "G46", "I64", "G45.4", "I67.0", "I67.1", "I67.2", "I67.4", "I67.5", "I67.6", "I67.7", "I67.8", "I67.9", "I68.1", "I68.2", "I68.8", "I69"]

Dementia_ICD9 = ["290"]
Dementia_ICD10 = ["F00", "F01", "F02", "F05.1"]

Pulmonary_disease_ICD9 = ["490", "491", "492", "493", "494", "495", "496", "500", "501", "502", "503", "504", "505"]
Pulmonary_disease_ICD10 = ["J40", "J41", "J42", "J44", "J43", "J45", "J46", "J47", "J67", "J44", "J60", "J61", "J62", "J63", "J66", "J64", "J65"]

Connective_tissue_disorder_ICD9 = ["710.0", "710.1", "710.4", "714.0", "714.1", "714.2", "714.81", "517.1", "725"]
Connective_tissue_disorder_ICD10 = ["M32", "M34", "M33.2", "M05.3", "M05.8", "M05.9", "M06.0", "M06.3", "M06.9", "M05.0", "M05.2", "M05.1", "M35.3"]

Peptic_ulcer_ICD9 = ["531", "532", "533", "534"]
Peptic_ulcer_ICD10 = ["K25", "K26", "K27", "K28"]

Liver_disease_ICD9 = ["571.2", "571.4", "571.5", "571.6"]
Liver_disease_ICD10 = ["K70.2", "K70.3", "K73", "K71.7", "K74.0", "K74.2", "K74.6", "K74.3", "K74.4", "K74.5"]

Diabetes_ICD9 = ["250.0", "250.1", "250.2", "250.3", "250.7"]
Diabetes_ICD10 = ["E10.9", "E11.9", "E13.9", "E14.9", "E10.1", "E11.1", "E13.1", "E14.1", "E10.5", "E11.5", "E13.5", "E14.5"]

Diabetes_complications_ICD9 = ["250.4", "250.5", "250.6"]
Diabetes_complications_ICD10 = ["E10.2", "E11.2", "E13.2", "E14.2", "E10.3", "E11.3", "E13.3", "E14.3", "E10.4", "E11.4", "E13.4", "E14.4"]

Paraplegia_ICD9 = ["342", "344.1"]
Paraplegia_ICD10 = ["G81", "G04.1", "G82.0", "G82.1", "G82.2"]

Renal_disease_ICD9 = ["582", "583.0", "583.1", "583.2", "583.3", "583.5", "583.6", "583.7", "583.4", "585", "586", "588"]
Renal_disease_ICD10 = ["N03", "N05.2", "N05.3", "N05.4", "N05.5", "N05.6", "N07.2", "N07.3", "N07.4", "N01", "N18", "N19", "N25"]

Cancer_ICD9 = ["14", "15", "16", "18", "170", "171", "172", "174", "175", "176", "179", "190", "191", "192", "193", "194", "195.0", "195.1", "195.2", "195.3", "195.4", "195.5", "195.8", "200", "201", "202", "203", "204", "205", "206", "207", "208"]
Cancer_ICD10 = ["C0", "C1", "C2", "C3", "C40", "C41", "C43", "C45", "C46", "C47", "C48", "C49", "C5", "C6", "C70", "C71", "C72", "C73", "C74", "C75", "C76", "C80", "C81", "C82", "C83", "C84", "C85", "C88.3", "C88.7", "C88.9", "C90.0", "C90.1", "C91", "C92", "C93", "C94.0", "C94.1", "C94.2", "C94.3", "C94.51", "C94.7", "C95", "C96"]

Metastatic_cancer_ICD9 = ["196", "197", "198", "199.0", "199.1"]
Metastatic_cancer_ICD10 = ["C77", "C78", "C79", "C80"]  

Severe_liver_disease_ICD9 = ["572.2", "572.3", "572.4", "572.8"]
Severe_liver_disease_ICD10 = ["K72.9", "K76.6", "K76.7", "K72.1"]

HIV_ICD9 = ["042", "043", "044"]
HIV_ICD10 = ["B20", "B21", "B22", "B23", "B24"]

ALL_ICD9 = ["410", "412", "428", "441", "443.9", "785.4", "V434", "430", "431", "432", "433", "434", "435", "436", "438", "290", "490", "491", "492", "493", "494", "495", "496", "500", "501", "502", "503", "504", "505", "710.0", "710.1", "710.4", "714.0", "714.1", "714.2", "714.81", "517.1", "725", "531", "532", "533", "534", "571.2", "571.4", "571.5", "571.6", "250.0", "250.1", "250.2", "250.3", "250.7", "250.4", "250.5", "250.6", "342", "344.1", "582", "583.0", "583.1", "583.2", "583.3", "583.5", "583.6", "583.7", "583.4", "585", "586", "588", "14", "15", "16", "18", "170", "171", "172", "174", "175", "176", "179", "190", "191", "192", "193", "194", "195.0", "195.1", "195.2", "195.3", "195.4", "195.5", "195.8", "200", "201", "202", "203", "204", "205", "206", "207", "208", "196", "197", "198", "199.0", "199.1", "572.2", "572.3", "572.4", "572.8", "042", "043", "044"]
ALL_ICD10 = ["I21", "I22", "I252", "I50", "I71", "I79.0", "I73.9", "R02", "Z95.8", "Z95.9", "I60", "I61", "I62", "I63", "I65", "I66", "G45.0", "G45.1", "G45.2", "G45.8", "G45.9", "G46", "I64", "G45.4", "I67.0", "I67.1", "I67.2", "I67.4", "I67.5", "I67.6", "I67.7", "I67.8", "I67.9", "I68.1", "I68.2", "I68.8", "I69", "F00", "F01", "F02", "F05.1", "J40", "J41", "J42", "J44", "J43", "J45", "J46", "J47", "J67", "J44", "J60", "J61", "J62", "J63", "J66", "J64", "J65", "M32", "M34", "M33.2", "M05.3", "M05.8", "M05.9", "M06.0", "M06.3", "M06.9", "M05.0", "M05.2", "M05.1", "M35.3", "K25", "K26", "K27", "K28", "K70.2", "K70.3", "K73", "K71.7", "K74.0", "K74.2", "K74.6", "K74.3", "K74.4", "K74.5", "E10.9", "E11.9", "E13.9", "E14.9", "E10.1", "E11.1", "E13.1", "E14.1", "E10.5", "E11.5", "E13.5", "E14.5", "E10.2", "E11.2", "E13.2", "E14.2", "E10.3", "E11.3", "E13.3", "E14.3", "E10.4", "E11.4", "E13.4", "E14.4", "G81", "G04.1", "G82.0", "G82.1", "G82.2", "N03", "N05.2", "N05.3", "N05.4", "N05.5", "N05.6", "N07.2", "N07.3", "N07.4", "N01", "N18", "N19", "N25", "C0", "C1", "C2", "C3", "C40", "C41", "C43", "C45", "C46", "C47", "C48", "C49", "C5", "C6", "C70", "C71", "C72", "C73", "C74", "C75", "C76", "C80", "C81", "C82", "C83", "C84", "C85", "C88.3", "C88.7", "C88.9", "C90.0", "C90.1", "C91", "C92", "C93", "C940", "C94.1", "C94.2", "C94.3", "C94.51", "C94.7", "C95", "C96", "C77", "C78", "C79", "C80", "K72.9", "K76.6", "K76.7", "K72.1", "B20", "B21", "B22", "B23", "B24"]

Osteoporosis_ICD10 = ["M80", "M80.0", "M80.0", "M81", "M81.0", "M81.6", "M81.8", "Z87.310" ]
Osteoporosis_ICD9 = ["733.0", "731.0", "733.0", "733.3", "733.7", "268.2"]

Mental_and_behavioral_disorders_due_to_psychoactive_substance_abuse_ICD10 = ["F1"]
Mental_and_behavioral_disorders_due_to_psychoactive_substance_abuse_ICD9 = ["291", "292", "303", "304", "305"]

Schizophrenia_schizotypal_delusional_and_other_nonmood_disorders_ICD10 = ["F2"]
Schizophrenia_schizotypal_delusional_and_other_nonmood_disorders_ICD9 = ["295", "297", "298.1", "298.3", "298.4", "298.8", "298.9", "301.22"]

Mood_affective_disorders_ICD10 = ["F3"]
Mood_affective_disorders_ICD9 = ["296", "298.0", "300.4", "301.10", "301.12", "301.13", "311", "625.4"]

Anxiety_dissociative_stressrelated_somatoform_and_other_nonpsychotic_mental_disorders_ICD10 = ["F4"]
Anxiety_dissociative_stressrelated_somatoform_and_other_nonpsychotic_mental_disorders_ICD9 = ["300.2","300.8","308","309.8", "298.2", "300.00",
                                                                                              "300.01", "300.02","300.09", "300.10", "300.11","300.12","300.13",
                                                                                              "300.14", "300.15","300.16","300.3", "300.5","300.6","300.7","300.9",
                                                                                              "306.0","306.1","306.2","306.3","306.4","306.50","306.52","306.53", "306.59",
                                                                                              "306.6", "306.7", "306.8", "306.9", "307.80", "307.89","309.0","309.1", "309.24",
                                                                                              "309.28","309.29","309.3","309.4","309.9","310.81","V40.2"]

PULMONARY_EMBOLISM_ACUTE_ICD10 = ["I26", "I26.0", "I26.01", "I26.02", "I26.09", "I26.9", "I26.90", "I26.92", "I26.99"]
PULMONARY_EMBOLISM_ACUTE_ICD9 = ["415", "415.0", "415.1", "415.11", "415.12", "415.13", "415.19"]

PULMONARY_EMBOLISM_CHRONIC_ICD10 = ["I27.82"]
PULMONARY_EMBOLISM_CHRONIC_ICD9 = ["416.2"]

ACUTE_DVT_LOWER_EXTREMITY_ICD10 = ["I82.4", "I82.40", "I82.401", "I82.402", "I82.403", "I82.409", "I82.41", "I82.411", "I82.412", "I82.413", "I82.419", "I82.42", "I82.421", "I82.422", "I82.423", "I82.429", "I82.43", "I82.431", "I82.432", "I82.433", "I82.439", "I82.44", "I82.441", "I82.442", "I82.443", "I82.449", "I82.49", "I82.491", "I82.492", "I82.493", "I82.499", "I82.4"]
ACUTE_DVT_LOWER_EXTREMITY_ICD9 = ["453.4", "453.40", "453.41", "453.42"]

CHRONIC_DVT_LOWER_EXTREMITY_ICD10 = ["I82.5", "I82.50", "I82.501", "I82.502", "I82.503", "I82.509", "I82.51", "I82.511", "I82.512", "I82.513", "I82.519", "I82.52", "I82.521", "I82.522", "I82.523", "I82.529", "I82.53", "I82.531", "I82.532", "I82.533", "I82.539", "I82.54", "I82.541", "I82.542", "I82.543", "I82.549", "I82.59", "I82.591", "I82.592", "I82.593", "I82.599", "I82.5"]
CHRONIC_DVT_LOWER_EXTREMITY_ICD9 = ["453.5", "453.50", "453.51", "453.52"]

ACUTE_DVT_UPPER_EXTREMITY_ICD10 = ["I82.6", "I82.60", "I82.601", "I82.602", "I82.603", "I82.609", "I82.61", "I82.611", "I82.612", "I82.613", "I82.619", "I82.62", "I82.621", "I82.622", "I82.623", "I82.629"]
ACUTE_DVT_UPPER_EXTREMITY_ICD9 = ["453.8", "453.82", "453.83", "453.84", "453.85", "453.86", "453.87", "453.89", "453.9"]

CHRONIC_DVT_UPPER_EXTREMITY_ICD10 = ["I82.7", "I82.70", "I82.701", "I82.703", "I82.709", "I82.71", "I82.711", "I82.712", "I82.713", "I82.719", "I82.72", "I82.721", "I82.722", "I82.723", "I82.729"]
CHRONIC_DVT_UPPER_EXTREMITY_ICD9 = ["453.7", "453.72", "453.73", "453.74", "453.75", "453.76", "453.77", "453.79"]

PHLEBITIS_AND_THROMBOPHLEBITIS_OF_LOWER_EXTREMITY_ICD10 = ["I80.1", "I80.10", "I80.11", "I80.12", "I80.13", "I80.2", "I80.20", "I80.21", "I80.22", "I80.23", "I80.23", "I80.29", "I80.201", "I80.202", "I80.203", "I80.209", "I80.211", "I80.212", "I80.213", "I80.219", "I80.221", "I80.222", "I80.223", "I80.229", "I80.231", "I80.232", "I80.233", "I80.239", "I80.291", "I80.292", "I80.293", "I80.299", "I80.3", "I82.4", "I82.5"]
PHLEBITIS_AND_THROMBOPHLEBITIS_OF_LOWER_EXTREMITY_ICD9 = ["451.1", "451.11", "451.19", "451.2"]

PHLEBITIS_AND_THROMBOPHLEBITIS_OF_UPPER_BODY_OR_EXTREMITY_ICD10 = ["I82.A", "I82.B", "I82.C", "I82.2", "I82.3", "I82.4", "I82.6", "I82.7", "I82.8", "I82.9", "I82.0"]
PHLEBITIS_AND_THROMBOPHLEBITIS_OF_UPPER_BODY_OR_EXTREMITY_ICD9 = ["451.81", "451.83", "451.84", "451.89", "453.0", "453.2", "453.3"]

UNSPECIFIED_PHLEBITIS_AND_THROMBOPHLEBITIS_ICD10 = ["I80.8", "I80.9"]
UNSPECIFIED_PHLEBITIS_AND_THROMBOPHLEBITIS_ICD9 = ["451.9"]

HISTORY_OF_DVT_ICD10 = ["Z86.718"]
HISTORY_OF_DVT_ICD9 = ["V12.51"]

# Adding new categories for Hepatitis C
# taken from https://echo.ucsf.edu/sites/g/files/tkssra3396/f/wysiwyg/HEPC5%20Diagnosis%20Codes%20Screening%20ToolKit%20for%20PMD.pdf
HEPATITIS_C_ICD10 = ["B17.10","B18.2","B19.20","Z86.19","Z22.52"]
HEPATITIS_C_ICD9 = ["070.41","070.44","070.51","070.54","070.70","070.71","V02.62"]
#TODO: Make Hep C List
# icd9 listed as well.


ICD10_Lists = [Acute_MI_ICD10,CHF_ICD10,Peripheral_vascular_disease_ICD10,CVA_ICD10,Dementia_ICD10,Pulmonary_disease_ICD10,Connective_tissue_disorder_ICD10,
                Peptic_ulcer_ICD10,Liver_disease_ICD10,Diabetes_ICD10,Diabetes_complications_ICD10,Paraplegia_ICD10,Renal_disease_ICD10,Cancer_ICD10,Metastatic_cancer_ICD10,Severe_liver_disease_ICD10,HIV_ICD10,ALL_ICD10,Osteoporosis_ICD10,Mental_and_behavioral_disorders_due_to_psychoactive_substance_abuse_ICD10,
                Schizophrenia_schizotypal_delusional_and_other_nonmood_disorders_ICD10,Mood_affective_disorders_ICD10,Anxiety_dissociative_stressrelated_somatoform_and_other_nonpsychotic_mental_disorders_ICD10,
                PULMONARY_EMBOLISM_ACUTE_ICD10,PULMONARY_EMBOLISM_CHRONIC_ICD10,ACUTE_DVT_LOWER_EXTREMITY_ICD10,CHRONIC_DVT_LOWER_EXTREMITY_ICD10,ACUTE_DVT_UPPER_EXTREMITY_ICD10,
                CHRONIC_DVT_UPPER_EXTREMITY_ICD10,PHLEBITIS_AND_THROMBOPHLEBITIS_OF_LOWER_EXTREMITY_ICD10,
                PHLEBITIS_AND_THROMBOPHLEBITIS_OF_UPPER_BODY_OR_EXTREMITY_ICD10,
                UNSPECIFIED_PHLEBITIS_AND_THROMBOPHLEBITIS_ICD10,HISTORY_OF_DVT_ICD10,HEPATITIS_C_ICD10]

ICD10_List_Names = ["Acute_MI_ICD10","CHF_ICD10","Peripheral_vascular_disease_ICD10","CVA_ICD10","Dementia_ICD10","Pulmonary_disease_ICD10","Connective_tissue_disorder_ICD10",
                "Peptic_ulcer_ICD10","Liver_disease_ICD10","Diabetes_ICD10","Diabetes_complications_ICD10","Paraplegia_ICD10","Renal_disease_ICD10","Cancer_ICD10","Metastatic_cancer_ICD10","Severe_liver_disease_ICD10","HIV_ICD10","ALL_ICD10","Osteoporosis_ICD10","Mental_and_behavioral_disorders_due_to_psychoactive_substance_abuse_ICD10",
                "Schizophrenia_schizotypal_delusional_and_other_nonmood_disorders_ICD10","Mood_affective_disorders_ICD10","Anxiety_dissociative_stressrelated_somatoform_and_other_nonpsychotic_mental_disorders_ICD10",
                "PULMONARY_EMBOLISM_ACUTE_ICD10","PULMONARY_EMBOLISM_CHRONIC_ICD10","ACUTE_DVT_LOWER_EXTREMITY_ICD10","CHRONIC_DVT_LOWER_EXTREMITY_ICD10","ACUTE_DVT_UPPER_EXTREMITY_ICD10",
                "CHRONIC_DVT_UPPER_EXTREMITY_ICD10","PHLEBITIS_AND_THROMBOPHLEBITIS_OF_LOWER_EXTREMITY_ICD10",
                "PHLEBITIS_AND_THROMBOPHLEBITIS_OF_UPPER_BODY_OR_EXTREMITY_ICD10",
                "UNSPECIFIED_PHLEBITIS_AND_THROMBOPHLEBITIS_ICD10","HISTORY_OF_DVT_ICD10","HEPATITIS_C_ICD10"]


ICD9_Lists = [Acute_MI_ICD9,CHF_ICD9,Peripheral_vascular_disease_ICD9,CVA_ICD9,Dementia_ICD9,Pulmonary_disease_ICD9,Connective_tissue_disorder_ICD9,
                Peptic_ulcer_ICD9,Liver_disease_ICD9,Diabetes_ICD9,Diabetes_complications_ICD9,Paraplegia_ICD9,Renal_disease_ICD9,Cancer_ICD9,Metastatic_cancer_ICD9,Severe_liver_disease_ICD9,HIV_ICD9,ALL_ICD9,Osteoporosis_ICD9,Mental_and_behavioral_disorders_due_to_psychoactive_substance_abuse_ICD9,
                Schizophrenia_schizotypal_delusional_and_other_nonmood_disorders_ICD9,Mood_affective_disorders_ICD9,Anxiety_dissociative_stressrelated_somatoform_and_other_nonpsychotic_mental_disorders_ICD9,
                PULMONARY_EMBOLISM_ACUTE_ICD9,PULMONARY_EMBOLISM_CHRONIC_ICD9,ACUTE_DVT_LOWER_EXTREMITY_ICD9,CHRONIC_DVT_LOWER_EXTREMITY_ICD9,ACUTE_DVT_UPPER_EXTREMITY_ICD9,
                CHRONIC_DVT_UPPER_EXTREMITY_ICD9,PHLEBITIS_AND_THROMBOPHLEBITIS_OF_LOWER_EXTREMITY_ICD9,
                PHLEBITIS_AND_THROMBOPHLEBITIS_OF_UPPER_BODY_OR_EXTREMITY_ICD9,
                UNSPECIFIED_PHLEBITIS_AND_THROMBOPHLEBITIS_ICD9,HISTORY_OF_DVT_ICD9,HEPATITIS_C_ICD9]

ICD9_List_Names = ["Acute_MI_ICD9","CHF_ICD9","Peripheral_vascular_disease_ICD9","CVA_ICD9","Dementia_ICD9","Pulmonary_disease_ICD9","Connective_tissue_disorder_ICD9",
                "Peptic_ulcer_ICD9","Liver_disease_ICD9","Diabetes_ICD9","Diabetes_complications_ICD9","Paraplegia_ICD9","Renal_disease_ICD9","Cancer_ICD9","Metastatic_cancer_ICD9","Severe_liver_disease_ICD9","HIV_ICD9","ALL_ICD9","Osteoporosis_ICD9","Mental_and_behavioral_disorders_due_to_psychoactive_substance_abuse_ICD9",
                "Schizophrenia_schizotypal_delusional_and_other_nonmood_disorders_ICD9","Mood_affective_disorders_ICD9","Anxiety_dissociative_stressrelated_somatoform_and_other_nonpsychotic_mental_disorders_ICD9",
                "PULMONARY_EMBOLISM_ACUTE_ICD9","PULMONARY_EMBOLISM_CHRONIC_ICD9","ACUTE_DVT_LOWER_EXTREMITY_ICD9","CHRONIC_DVT_LOWER_EXTREMITY_ICD9","ACUTE_DVT_UPPER_EXTREMITY_ICD9",
                "CHRONIC_DVT_UPPER_EXTREMITY_ICD9","PHLEBITIS_AND_THROMBOPHLEBITIS_OF_LOWER_EXTREMITY_ICD9",
                "PHLEBITIS_AND_THROMBOPHLEBITIS_OF_UPPER_BODY_OR_EXTREMITY_ICD9",
                "UNSPECIFIED_PHLEBITIS_AND_THROMBOPHLEBITIS_ICD9","HISTORY_OF_DVT_ICD9","HEPATITIS_C_ICD9"]

df_icd10 = pd.DataFrame(np.transpose([ICD10_List_Names,ICD10_Lists]),columns=("Listname","List"))
df_icd9 = pd.DataFrame(np.transpose([ICD9_List_Names,ICD9_Lists]),columns=("Listname","List"))



#### BARDIYA SAVE
# df_icd9_clean = pd.DataFrame(np.transpose([pd.Series(['_'.join(name.split('_')[:-1]) for name in ICD9_List_Names]), ICD9_Lists]),columns=("Comorbidity","ICD9_List"))
# df_icd9_clean.to_csv('ICD9.csv',index=False)

# df_icd10_clean = pd.DataFrame(np.transpose([pd.Series(['_'.join(name.split('_')[:-1]) for name in ICD10_List_Names]), ICD10_Lists]),columns=("Comorbidity","ICD10_List"))
# df_icd10_clean.to_csv('ICD10.csv',index=False)
#####

matched_ICDs = {"icd10":[],"icd9":[]}
empty_matchers = {"icd10":[],"icd9":[]}


# This function takes each comorbidity and checks what categories are applicable.
# It will populate an existing dictionary 'matched_ICDs' with pairs of codes and their corresponding
# categories so that the dictionary can be used to label all of the diagnosis entries for the patients.
def comorbLabeler(x,mode="icd10"):
    icdmode = {"icd10":df_icd10,"icd9":df_icd9}
    icdmode_allname = {"icd10":"ALL_ICD10","icd9":"ALL_ICD9"}
    ICDs = x
    if ICDs not in matched_ICDs[mode]:
        anymatch = False
        for index, each_list in enumerate(icdmode[mode]["List"]):
            if icdmode[mode].iloc[index]["Listname"] == icdmode_allname[mode]:
                continue
            matched = False
            for term in each_list:
                if str(term) in str(ICDs):
                    matched = True
                    anymatch = True
                    break
            if matched:
                empty_matchers[mode].append([ICDs,icdmode[mode].iloc[index]["Listname"]])
        if anymatch:
            matched_ICDs[mode].append(ICDs)
            
tqdm.pandas()
# matching icd10
print("Matching icd10 and icd9")
df_dias_icd10.progress_apply(lambda x: comorbLabeler(x["Code.pastdia"],mode="icd10"),axis=1)
df_dias_icd9.progress_apply(lambda x: comorbLabeler(x["Code.pastdia"],mode="icd9"),axis=1)

# matching icd9 - foregoing this for now given the proportion of icd10 to icd9 codes.
#print("Matching icd9")
#df_dias_icd9.progress_apply(lambda x: comorbLabeler(x["Code.pastdia"],mode="icd10"),axis=1)

# Found the matching categoriesso then now need to do the empty matcher with the dataframe.
matchframe10 = pd.DataFrame(empty_matchers["icd10"],columns=["Code.pastdia","ComorbCategory"])   
matchframe9 = pd.DataFrame(empty_matchers["icd9"],columns=["Code.pastdia","ComorbCategory"])   

df_dias_icd10_categorized = pd.merge(df_dias_icd10,matchframe10,on="Code.pastdia",how="left")
df_dias_icd9_categorized = pd.merge(df_dias_icd9,matchframe9,on="Code.pastdia",how="left")

#%% Calculating the CCI:

# dataframe of the categorized diagnoses
allmatches10 = df_dias_icd10_categorized[~pd.isna(df_dias_icd10_categorized["ComorbCategory"])]
allmatches9 = df_dias_icd9_categorized[~pd.isna(df_dias_icd9_categorized["ComorbCategory"])]

# initial dataframe of the patients and their initial procedure dates
df_cat = df

#for each comorbidity category, add a column to the initial dataframe of patients
# with date of most recent (before procedure) qualifying comorbidity date if any
for cat in tqdm(ICD10_List_Names):
    
    #filter on each category
    catmatch10 = allmatches10[allmatches10["ComorbCategory"] == cat]
    catmatch9 = allmatches9[allmatches9["ComorbCategory"] == cat.replace("ICD10","ICD9")]
    
    catmatch = pd.concat([catmatch10,catmatch9])
    
    df_cat = pd.merge(df_cat,catmatch[["EMPI","Date.dia","Date.pastdia"]],on=["EMPI","Date.dia"],how="left")
    
    #sort by recency
    df_cat = df_cat.sort_values("Date.pastdia")
    
    #remove duplicates
    df_cat = df_cat.drop_duplicates(subset=duplicate_columns)
    df_cat = df_cat.rename(columns={"Date.pastdia":cat.replace("ICD10","diagnosis")})

# generate CCI weight table.
CCI_Weight = {}

for i in range(17):
    if i<10:
        CCI_Weight[ICD10_List_Names[i]] = 1
    elif i < 14:
        CCI_Weight[ICD10_List_Names[i]] = 2
    elif i< 15:
        CCI_Weight[ICD10_List_Names[i]] = 3
    else:
        CCI_Weight[ICD10_List_Names[i]] = 6

#%% Change CCI_Weight values

CCI_Weight["Metastatic_cancer_ICD10"]=6
CCI_Weight["Severe_liver_disease_ICD10"]=3

#calculate CCI based on non empty values for each qualifying category
df_cat["CCI"] = 0
for cat in ICD10_List_Names[0:17]:
    df_cat["CCI"] = df_cat["CCI"] + ((~pd.isna(df_cat[cat.replace("ICD10", "diagnosis")])).astype(int)*CCI_Weight[cat])

#%%
print(df_cat.keys())

##%% Adding age to CCI:

## Age group CCI 
#bins= [0,50,60,70,80,120]
#labels = [0,1,2,3,4]
#df_cat['AgeGroup'] = pd.cut(df_cat['AgeGroup'], bins=bins, labels=labels, right=False)
#df_cat['AgeGroup'] = pd.to_numeric(df_cat['AgeGroup'])

# Adding age points to CCI
#df_cat['Age_CCI'] = df_cat['CCI'] + df_cat['AgeGroup']

#%% Determine specific categories for this study:

# Diabetes
df_cat["Diabetes"] = df_cat["Diabetes_diagnosis"]

# Hepatitis C
df_cat["Hepatitis_C"] = df_cat["HEPATITIS_C_diagnosis"]

# Liver Disease
df_cat["Liver_Disease"] = df_cat.apply(lambda x: x["Severe_liver_disease_diagnosis"] if not pd.isna(x["Severe_liver_disease_diagnosis"]) else x["Liver_disease_diagnosis"],axis=1)

# HIV
df_cat["HIV"] = df_cat["HIV_diagnosis"]

#%% Subset to the desired columns:
    
df_subset = df_cat[list(df_cat.columns)[:8] + list(df_cat.columns)[40:]]

print(len(df_subset))

#%%
SAVE = True

if SAVE:
    df_subset.to_csv(os.path.join(path_saving_root,'CleanedData_CCI.csv'), index=False)  






    
    
    