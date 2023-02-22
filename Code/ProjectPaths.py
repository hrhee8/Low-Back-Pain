# -*- coding: utf-8 -*-
"""
Created on Thu Dec 16 08:04:18 2021

This code has all of the paths and names specific to this project.

@author: David Shin and Bardiya Akhbari
"""

import os
import sys
from sys import platform


#%% Project Specific Variables
project_name        = 'Low Back Pain'

Desktop = os.path.join(os.path.expanduser("~"), "Desktop")

#path_root           = r'AI registries/Projects' # this is the project root folder (where project name folder exists)

path_saving_root    = os.path.join(Desktop, "LBP_Revision")

path_inclusion_crit = os.path.join(path_saving_root,'inclusion_codes_LBP.xlsx')
path_inclusion_crit2 = os.path.join(path_saving_root,'inclusion_codes_DJD.xlsx')
path_inclusion_crit3 = os.path.join(path_saving_root,'inclusion_codes_fracture.xlsx')
path_inclusion_crit4 = os.path.join(path_saving_root,'inclusion_codes_other.xlsx')
path_inclusion_crit5 = os.path.join(path_saving_root,'inclusion_codes_scoliosis.xlsx')

path_rpdr_files     = os.path.join('Data','RPDR') # location that .txt resides there 

namecode            = 'JHS26_20220324_144722' # this is the first part of the RPDR text files

#############################################################
# make sure to change this!!!! file name with year!!!
save_filename       = 'Filtered_ICD_CPT_Dataset_exclusion_2021.h5'


#%% Dictionary for datatype
dtype_dict = {'EMPI': str, 'MGH_MRN': str}


#%% Importing rpdr
## COPY & PASTE THIS TO YOUR PROJECT
os_mode = ""    
verification_file = r':/'+path_root+'/General Code/RPDR Data Processing/RPDR_Processor.py'


# checks whether or not the system is windows or mac
if platform == "win32":
    os_mode = "Windows"
    # todo: read all of the connected mapped network drive letters
    # and determine which one is the MGH-ORAI and adjust the path stem accordingly.
    for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        testpath = letter+verification_file
        if os.path.exists(letter+verification_file):
            basepath = testpath.replace('/RPDR_Processor.py','')
elif platform == "darwin":
    os_mode = "Mac"
    basepath = '/Volumes/MGH-ORAI/'+path_root+'/General Code/RPDR Data Processing'
    
if basepath not in sys.path:
    sys.path.append(basepath)
    
# parse drive stem
drivestem = basepath.split(path_root)[0]

print("base path is: "+basepath)
print("drivestem is: "+drivestem)
    


#%% User specified parameters
path_to_project = os.path.join(path_root, project_name)
drivepath       = os.path.join(path_to_project, path_rpdr_files)


textfile_path   = os.path.join(drivestem, drivepath).replace(os.path.sep, '/')


h5_filepath     = textfile_path + '/' + save_filename
print('stored data is at\n{}'.format(h5_filepath))     


#%% Check the RPDR exported data and access the filecodes
filecodes_existing = []
for file in os.listdir(textfile_path):
    if file.endswith(".txt.exe"):
        temp_code = file[-11:-8]
        if temp_code != 'Log':
            filecodes_existing.append(temp_code)