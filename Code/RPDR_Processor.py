# -*- coding: utf-8 -*-
"""
Created on UNK

@author: David Shin
@code review: [In progress] Bardiya Akhbari
@edited: Hyunjoon Rhee

"""

# importing libraries
import pandas as pd
import pickle
import datetime
import re
import os, sys, getopt, threading, openpyxl

testing_mode = 0

helptext = """functionality:

python RPDR_Processor.py [options]

[options]:

-h		: help
-e		: process (open) executables (for creating txt files)
-s FILENAME	: specify a single file (may be a text file to convert or an exe to run)
-p		: process TEXT files and convert to xlsx (this is the default behavior)
-o		: overwrite mode - default behavior is not to overwrite existing files
-z		: just print the current directory (mainly for testing)
-d DIRNAME	: process files in a given directory
-n NAMECODE	: process only files with names beginning in NAMECODE

eg:
    
python RPDR_Processor.py -o -d SOMEDRIVE://folder/subfolder -n abcdefg100
"""

keymode = "infer"

def timenow():
    ### get the current time --- this code is to check the runtime/saving time
    timeloaded = datetime.datetime.now()
    timeloadedstr = "["+timeloaded.strftime("%H:%M:%S")+"]"
    return timeloaded,timeloadedstr
    
def timediffstr(timestart,timeloaded):
    ### to calculate the time difference between the opening and saving
    loadtime = timeloaded - timestart
    hours, rem = divmod(loadtime.seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    loadtimestr = '{:02d}h {:02d}m {:02}s'.format(hours,minutes,seconds)
    return loadtimestr
    

    
#%% Main Function that Reads Each File and Convert it to a Dataframe
def RPDRParser(filepath):
    ### to read one of the files
    numtabs = 0
    nametabs = []
    
    # read the first line:
    with open(filepath, 'r', encoding='utf8') as readfile:
        firstline = readfile.readline() # reading the first line in the file
        numtabs = firstline.count('|') # to check how many column headers exist in the file
        nametabs = firstline.strip().split('|') # to access the name of the columns
        
        txt = readfile.read() # reading the rest of the document --- since we're opening the full file, this one takes a long time usually (depending on the filesize)

        timestart, timestartstr = timenow()
        print(f"{timestartstr} Parsing Begins...")
        txt = re.sub('\|\|','|NaN|',txt)
        txt = re.sub('\|\|','|NaN|',txt)
        timestart, timestartstr = timenow()
        print(f"{timestartstr} Parsing: no character to NaN replacement is done..")
        
        preparse = re.findall('(([^\|]*\|[^\|]*){'+str(numtabs)+'}(\n|\Z))', txt)
        
        timestart, timestartstr = timenow()
        print(f"{timestartstr} Parsing: converting the text file to pandas dataframe..")
        parsed = [t[0].split('|') for t in preparse]
        
    return pd.DataFrame(parsed, columns=nametabs, dtype=str)
    


def RPDRParser_line(filepath):
    ### to read one of the files
    numtabs = 0
    nametabs = []
    parsed = []
    
    # read the first line:
    problem_lines =[]
    with open(filepath, 'r', encoding='utf8') as readfile:
        firstline = readfile.readline() # reading the first line in the file
        numtabs = firstline.count('|') # to check how many column headers exist in the file
        nametabs = firstline.strip().split('|') # to access the name of the columns
        
        #txt = readfile.read() # reading the rest of the document --- since we're opening the full file, this one takes a long time usually (depending on the filesize)
        counter = 0
        for line in readfile: 
            
            txt = re.sub('\|\|','|NaN|', line)
            txt = re.sub('\|\|','|NaN|', txt)
            
            try:
                assert len(txt.split('|')) == len(nametabs)
            except:
                problem_lines.append(txt)
                print('--> WARNING: This is the last line or there is a problem here L{}'.format(counter))
                continue
                    
            
            preparse = re.findall('(([^\|]*\|[^\|]*){'+str(numtabs)+'}(\n|\Z))', txt)
     
            parsed.append([t[0].split('|') for t in preparse][0])
        
            counter += 1
            if counter % 1000000 == 0:
                print('Processed up to line {}...'.format(counter))
                
    return pd.DataFrame(parsed, columns=nametabs, dtype=str)



#%% Reads RPDR text file and saves as an excel file.
def processRPDRTextFile(INPUT, OUTPUT):
    
    # read in the file
    filename = INPUT+'.txt'
    
    timestart,timestartstr = timenow()
    print(f"{timestartstr} Reading file: {filename} ...")
    
    # LOAD: load in the file as a data frame
    data = pd.read_csv(INPUT+'.txt',sep="|",dtype=str)
    
    timeloaded,timeloadedstr = timenow()
    loadtimestr = timediffstr(timestart,timeloaded)
    
    print(f"{timeloadedstr} File {filename} successfully read in! ({loadtimestr} elapsed)\n")
    
    
    # SAVE: save this data as a dataframe in an hdfstore
    #savename = OUTPUT+'.pkl'
    savename = OUTPUT
    
    ptimestart = datetime.datetime.now()
    ptimestartstr = "["+ptimestart.strftime("%H:%M:%S")+"]"
    
    print(f"{ptimestartstr} Saving to: {savename} ...")
    
    #with open(picklename,'wb') as file_obj:
    with pd.HDFStore('dataFrames.h5') as store:
        store[OUTPUT] = data
        #data.to_pickle(picklename,compression="gzip")
   
    ptimeloaded = datetime.datetime.now()
    ptimeloadedstr = "["+ptimeloaded.strftime("%H:%M:%S")+"]"
    
    ploadtime = ptimeloaded - ptimestart
    hours, rem = divmod(ploadtime.seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    ploadtimestr = '{:02d}h {:02d}m {:02}s'.format(hours,minutes,seconds)

    print(f"{ptimeloadedstr} File {savename} successfully saved! ({ploadtimestr} elapsed)\n")
    
    totalloadtime = ptimeloaded - timestart
    
    hours, rem = divmod(totalloadtime.seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    totalloadtimestr = '{:02d}h {:02d}m {:02}s'.format(hours,minutes,seconds)

    print(f"{ptimeloadedstr} File {filename} successfully processed! ({totalloadtimestr} elapsed in total)\n")
       
        
       
    # # ELSE:
    # if len(data) ==  0:
    #     #print("reading file")
    #     with open(INPUT+'.txt',"r",encoding="utf-8") as f:
    #         firstLine = 1 # whether reading first line
    #         colNum = 0 # number of columnsand
    #         dataList = [] # list to store data
    #         rowList = [] # list to store rows
    #         header = [] # list to store header row
    #         count = 0 #which row element you are reading
    
    #         #iterate to read file lines
    #         line = f.readline()
    #         while True:
                
    #             if firstLine == 1: #process the header line
    #                 #DEBUG
    #                 #print("processing first line")
    #                 firstLine = 0
    #                 header = line.split('|')
    #                 colNum = len(header)
                    
    #                 line = f.readline()
    #             else:
    #                 #starting to read elements for this entry
    #                 if line and count == 0: #reading a new line
    #                     newline = line.split('|')
    #                     rowList = rowList + newline
    #                     count = len(rowList)
                        
    #                     line = f.readline()
    
    #                 #reading intermediate elements
    #                 while line and count < colNum: # still reading elements
    #                     midline = line.split('|')
    #                     continuingElement = midline[0]
    #                     remainingElements = midline[1:]
    
    #                     rowList[count-1] = rowList[count-1] + continuingElement #update element being read
    
    
    #                     rowList = rowList + remainingElements #add any new elements
    #                     count = len(rowList) #update number of 
                        
    #                     line = f.readline()
    
    #                 #reading last element or done with line
    #                 while line and count == colNum: # reading/checking for the last element.
    #                     lastElementLine = line.split('|')
    
    #                     if len(lastElementLine) > 1:
    #                         break #finished reading the last element of this row
                            
    #                     else:
    #                         #keep adding to this line
    #                         rowList[count-1] = rowList[count-1] + lastElementLine[0]
    #                         line = f.readline()
                            
    #                 #finished reading line
                    
    #                 #trim whitespace/newlines off of the last cell in the row being read.
    #                 rowList[count-1] = rowList[count-1].rstrip()
                            
    #                 #add the completed row to the data list.
    #                 dataList.append(rowList.copy()) #add the completed row into the data list
                    
    #                 #reset row reading variables (next line should already be read in)
    #                 count = 0 
    #                 rowList = []
    
    #                 if not line:
    #                     #print("creating dataframe from the list")
    #                     dataframe = pd.DataFrame(dataList,columns=header)
    #                     #writing
    #                     savename = OUTPUT+'2.pkl'
    #                     print(f"Saving {savename}...")
                        
    #                     #Note: Saving these files as pickles now.
    #                     store = pd.H
    #                     dataframe.to_pickle(savename)
                            
    #                     print(f"Finished saving {savename}.")
    #                     break


def processSaveRPDRText(textfile_path, namecode, filecodes, filterkeys, filterEMPIs, h5_filepath, save_csv = True):
    
    processingTimes = [["Filecode","Key","Type","Size","Duration"]]
    for i,filecode in enumerate(filecodes):
    
        # first, load in the data file.
        # data = pd.read_csv(namecode+'_'+filecode+'.txt',sep='|',dtype=str,lineterminator='\n')
        
        
        filename = namecode + '_' + filecode + '.txt'
        datatimestart,datatimestartstr = timenow()
        print(f"{datatimestartstr} Beginning processing of {filename}, {str(i+1)} of {str(len(filecodes))}...\n")
        
        filetimestart, filetimestartstr = timenow()
        print(f"{filetimestartstr} Reading file: {filename} ...")
        
        filepath = '/'.join([textfile_path, filename])
        
        if filecode == 'Lab':
            print('------------>  Reading Lab Data Specific Code')
            data = RPDRParser_line(filepath)
        else:
            # Reading the filepath here
            data = RPDRParser(filepath)
        
        filetimeloaded,filetimeloadedstr = timenow()
        fileloadtimestr = timediffstr(filetimestart,filetimeloaded)
        print(f"{filetimeloadedstr} File {filename} successfully read in! ({fileloadtimestr} elapsed)\n")
    
        
        # update runtime table
        size = os.path.getsize(filepath)
        processingTimes.append([filecode,'N/A','ReadTextfile',str(size),fileloadtimestr])
        
        for key in filterkeys:
            # for each of the empi sets by which to filter the data file, filter and save.
            
            keyfilecode = key+filecode
            
            realtimestart,realtimestartstr = timenow()
            print(f"{realtimestartstr} Beginning processing of {keyfilecode}...\n")
            
            empiset = filterEMPIs[key]
            
            # Filtering
            
            timestart,timestartstr = timenow()
            print(f"{timestartstr} Filtering: Beginning filtering of {keyfilecode}...")
            
            # execute filtering
            filtered_data = data[data['EMPI'].isin(empiset)]
            
            timeloaded,timeloadedstr = timenow()
            loadtimestr = timediffstr(timestart,timeloaded)
            print(f"{timeloadedstr} Filtering: {keyfilecode} successfully filtered! ({loadtimestr} elapsed)\n")
            
            # update runtime table
            processingTimes.append([filecode,key,'Filtering','N/A',loadtimestr])
            
            # Save the filtered data within the HDFStore 
                    
            timestart,timestartstr = timenow()
            print(f"{timestartstr} Saving in HDFStore: Storing {keyfilecode} in h5 file...")
            
            with pd.HDFStore(h5_filepath) as store:
                store[key+filecode] = filtered_data
            
            timeloaded,timeloadedstr = timenow()
            loadtimestr = timediffstr(timestart,timeloaded)
            print(f"{timeloadedstr} Saving in HDFStore: {keyfilecode} successfully saved in h5 file! ({loadtimestr} elapsed)\n")
            
            # update runtime table
            processingTimes.append([filecode,key,'SaveHDFStore','N/A',loadtimestr])
        
            
            
            if save_csv:
                # Saving as a csv file
                timestart,timestartstr = timenow() 
                print(f"{timestartstr} Saving as csv: Saving {keyfilecode}...")
                
                csvpath = os.path.join(textfile_path,key+filecode+'.csv')
                filtered_data.to_csv(csvpath,index=False)
                
                timeloaded,timeloadedstr = timenow()
                loadtimestr = timediffstr(timestart,timeloaded)
                print(f"{timeloadedstr} Saving as csv: {keyfilecode} successfully saved as csv! ({loadtimestr} elapsed)\n")
                
                # update runtime table
                size = os.path.getsize(csvpath)
                processingTimes.append([filecode,key,'SaveCSV',size,loadtimestr])
                
                
            # mark end of processing for this key+filecode pair    
            realtimeloaded,realtimeloadedstr = timenow()
            realloadtimestr = timediffstr(realtimestart,realtimeloaded)
            print(f"{realtimeloadedstr} Processing of {keyfilecode} complete! ({realloadtimestr} elapsed)\n")
                
            # update runtime table
            processingTimes.append([filecode,key,'TotalKeyFilecode','N/A',realloadtimestr])
            

        # mark end of processing this data type:
        datatimeloaded, datatimeloadedstr = timenow()
        dataloadtimestr = timediffstr(datatimestart,datatimeloaded)
        print(f"{datatimeloadedstr} Completed processing of {filename}, {str(i+1)} of {str(len(filecodes))}! ({dataloadtimestr} elapsed)\n")
    
        # update runtime table
        processingTimes.append([filecode,'N/A','TotalFilecode','N/A',dataloadtimestr])

            
    return pd.DataFrame(processingTimes[1:], columns=processingTimes[0])


#%% process series of files in a directory
def processNeedFile(name,mode):
    realName = name + modeExtension(mode)
    #DEBUG
    #print("check, name:{name},mode:{mode}".format(name=name,mode=mode))
    if mode == 1:
        try:
            threading.Thread(target=os.system,args=(realName,)).start()
            #os.system(realName)
        except:
            print("Could not run {filename}".format(filename=realName))
    else:
        processRPDRTextFile(name,name)

def singleNameProcess(name):
    if name.endswith(".txt"):
        return name[:-4], 0
    elif name.endswith(".exe"):
        return name[:-4], 1
    else:
        print('Single filename must end with .txt or .exe')
        sys.exit(2)
        
def modeOutputExtension(mode):
    if mode == 1:
        return ""
    else:
        return ".xlsx"

def checkExists(name,mode):
    return os.path.exists(name+modeOutputExtension(mode))

def modeExtension(mode):
    if mode == 1:
        return '.exe'
    else:
        return '.txt'

def main(argv):
    execute = 0
    process = 0
    single = 0
    singleName = ""
    directory = os.getcwd()
    namecode = ""
    overwrite = 0
    printdir = 0
    
    try:
        opts, args = getopt.getopt(argv,"ohepzs:d:n:",["overwrite","help","executables","process","printdir","single","directory=","namecode="])
    except getopt.GetoptError:
        print(helptext)
        sys.exit(2)
    
    #parse command line arguments
    for opt, arg in opts:
        if opt in ("-h","--help"):
            print(helptext)
            sys.exit()
        elif opt in ("-e","--executables"):
            execute = 1
        elif opt in ("-s","--single"):
            single = 1
            singleName = arg
        elif opt in ("-p","--process"):
            process = 0
        elif opt in ("-o","--overwrite"):
            overwrite = 1
        elif opt in ("-z","--printdir"):
            printdir = 1
        elif opt in ("-d","--directory"):
            directory = arg.replace('\\','/')
            try:
                os.chdir(directory)
            except:
                print('Error - {direc} cannot be opened'.format(direc = directory))
                sys.exit(2)
        elif opt in ("-n","--namecode"):
            namecode = arg
    
    mode = 0 # mode 1 is executables. mode 0 is processing.
    
    if execute and process:
        print('Error - cannot pass both execute and process commands')
        sys.exit(2)
    elif execute:
        # open the executables
        mode = 1
        
    if printdir == 1:
        print(os.getcwd())
    elif single:
        try:
            newname, mode = singleNameProcess(singleName)
            processNeedFile(newname, mode)
        except:
            print("Error - single file {file} failed to process".format(file = singleName))
            sys.exit(2)
    else:
        names = os.listdir()
        needFiles = []
        for name in names:
            if name.endswith(modeExtension(mode)) and name.startswith(namecode):
                newName = name[:-4]
                if overwrite == 1 or not checkExists(newName,mode):
                    needFiles.append(newName)
        
        for i,filename in enumerate(needFiles):
            try:
                print("Processing {filename}, {index} of {total} files".format(filename=filename+modeExtension(mode),index=str(i),total=str(len(needFiles))))
                processNeedFile(filename,mode)
            except:
                print("Error - {file} failed to process".format(file = filename+modeExtension(mode)))

# if __name__ == "__main__":
#     # execute only if run as a script
#     main(sys.argv[1:])
# else:
#     main([])
