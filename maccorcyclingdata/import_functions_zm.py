import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from importlib import reload
from sys import path

def import_maccor_data( file_path = '', file_name = '' , import_from_pickle = False , pickle_name = '', flavor = 'testdata', test_id = np.nan):
  '''
  This imports Maccor data from csv or pickle.  Give this function the file path and file name of 
  the big .txt file that is exported.  Make sure the data is exported as follows:

  - Tab delimited
  - Current as negative

  The function arguments are:

  file_path - file path to the raw data .txt or pickle
  file_name - file name of the raw data or pickle.
  pickle_name - pickle name holding the data
  import_from_pickle - a flag that tells us whether we are trying to import from a pickle.
  type - flag that tells whether we are importing test data ('testdata') or cycle stats data ('cyclestats')

  We also rename all of the headings to meet our conventions
  '''
  
  # If we are told to import from a pickle, see if it exists. If it does, import that data from there.
  if import_from_pickle:
    if os.path.exists(file_path + pickle_name) != True:
      print("Pickel path+name does not exisit!")
      return None
    else:
      df_out = pd.read_pickle(file_path + pickle_name)

  # See if the file path exists. If it does, load the raw data in and clean it.
  else:
    if os.path.exists(file_path + file_name) != True:
      print("File path+name does not exisit!")
      return None
    else:
      if flavor == 'testdata':
        df_raw = pd.read_csv(file_path+file_name, sep='\t', header = 2)  
        df_out = _clean_maccor_testdata(df_raw, test_id)
      elif flavor == 'cyclestats':
        df_raw = pd.read_csv(file_path+file_name, sep='\t', header = 8)
        df_out = _clean_maccor_cyclestats(df_raw, test_id)
      else: 
        print("Don't know that flavor of test data!")
        return None

  return df_out

def _clean_maccor_cyclestats(df_in, test_id):
  '''
  Takes in a raw imported maccor cycle stats data and cleans it. 

  df_in - The data frame to be cleaned. 
  test_id - The corresponding 'test_id' in the database
  '''

  # Create of copy of the df that we will modify
  df = df_in.copy()

  # Drop all unnamed columns
  unnamed_columnn_list = df.filter(like='Unnamed', axis=1).columns
  for unnamed_columnn in unnamed_columnn_list:
    if unnamed_columnn in df.columns:
        df = df.drop(columns=unnamed_columnn)

  # Other columns to drop that we don't want 
  columns_to_drop = ['Cycle Type', 'DCIR', 'Date']
  for column in columns_to_drop:
    if column in df.columns:
      df = df.drop(columns=column)

  # These are the headings that we will rename if they are in the stats file file 
  columns_to_rename =  [ ['Cycle','cycle'],['Test Time','test_time_s'],['Current','maccor_min_current_ma'],
                      ['Voltage','maccor_min_voltage_mv'],['AH-IN','maccor_charge_capacity_mah'],['AH-OUT','maccor_discharge_capacity_mah'],
                      ['WH-IN','maccor_charge_energy_wh'],['WH-OUT','maccor_discharge_energy_wh'],['T1_Start','maccor_charge_thermocouple_start_c'],
                      ['T1_End','maccor_charge_thermocouple_end_c'],['T1_Min','maccor_charge_thermocouple_min_c'],['T1_Max','maccor_charge_thermocouple_max_c'],
                      ['T1_Start.1','maccor_discharge_thermocouple_start_c'],['T1_End.1','maccor_discharge_thermocouple_end_c'],['T1_Min.1','maccor_discharge_thermocouple_min_c'],
                      ['T1_Max.1','maccor_discharge_thermocouple_max_c'],['ACR','acr_ohm']
                      ]
  for column in columns_to_rename:
    if column[0] in df.columns:
      df.rename(columns={column[0]:column[1]}, inplace=True)

  # Convert the time from string of format of DAY HOUR:MINUTE:SECTON to elapsed time in seconds
  if 'test_time_s' in df.columns:
    df.test_time_s = _string_to_int(df.test_time_s)
  
  # Change to the correct units
  if 'maccor_min_current_ma' in df.columns:
    df.maccor_min_current_ma = df.maccor_min_current_ma * 1e3
  if 'maccor_min_voltage_mv' in df.columns:
    df.maccor_min_voltage_mv = df.maccor_min_voltage_mv * 1e3
  if 'maccor_charge_capacity_mah' in df.columns:
    df.maccor_charge_capacity_mah = df.maccor_charge_capacity_mah #* 1e3
  if 'maccor_discharge_capacity_mah' in df.columns:
    df.maccor_discharge_capacity_mah = df.maccor_discharge_capacity_mah #* 1e3

  # Add a column for the test_id, if it is defined. Note we need this to upload data to the database.
  df['test_id'] = np.ones(df.shape[0],dtype=np.int8) * test_id

  return df

def _clean_maccor_testdata(df_in, test_id = '', timezone = 'America/Los_Angeles'):
  '''
  Takes in a raw imported maccor csv clean it. 

  df_in - The data frame to be cleaned. 
  test_id - The corresponding 'test_id' in the database
  timezone - The time zone the data was collected in
  '''

  # Create of copy of the df that we will modify
  df = df_in.copy()

  # Drop all unnamed columns
  unnamed_columnn_list = df.filter(like='Unnamed', axis=1).columns
  for unnamed_columnn in unnamed_columnn_list:
    if unnamed_columnn in df.columns:
        df = df.drop(columns=unnamed_columnn)

  # Other columns to drop that we don't want 
  columns_to_drop = ['ACR','DCIR']
  for column in columns_to_drop:
    if column in df.columns:
      df = df.drop(columns=column)

  # These are the headings that we will rename if they are in the stats file file 
  columns_to_rename =  [ ['Cyc#','cycle'],['Step','step'],['TestTime(s)','test_time_s'],
                         ['StepTime(s)','step_time_s'],['Capacity(Ah)','capacity_mah'],['Watt-hr','energy_wh'],
                         ['Current(A)','current_ma'],['Voltage(V)','voltage_mv'],['DPt Time','datetime'],
                         ['Temp 1','thermocouple_temp_c'],['EV Temp','ev_temp_c']
                         ]
  for column in columns_to_rename:
    if column[0] in df.columns:
      df.rename(columns={column[0]:column[1]}, inplace=True)

  # Sometimes values in watthr are non-numeric, so we need to turn it to NaN before changing data type
  #print (df[ pd.to_numeric(df['watthr'], errors='coerce').isnull()])
  if 'energy_wh' in df.columns:
    df['energy_wh'] = pd.to_numeric(df['energy_wh'], errors='coerce')

  # Get the datetime in the correct format and add column for unixtime
  df.datetime = pd.to_datetime( df.datetime , utc = True)
  df.datetime = df.datetime.dt.tz_convert(timezone)
  df['unixtime_s'] = (df.datetime.dt.tz_convert('UTC') - pd.Timestamp("1970-01-01",tz='UTC')) // pd.Timedelta('1s')

  # Change the units on everything
  # Note we don't need to convert capacity because it's already mah. Maccor just mislabels it (as of 5/05/2020)
  df.voltage_mv = df.voltage_mv * 1e3

  # Add a column for the test_id, if it is defined. Note we need this to upload data to the database.
  df['test_id'] = np.ones(df.shape[0],dtype=np.int8) * test_id

  return df

'''
This is a function we call for importing BAE data. 

The function arguments are as follows:


file path - The file path to where the data is kept. Assuming all data ends in .csv extension
import_from_pickle - A bool that delcares whether or not we should reload from a pickle (contained with the same file) or the data within the file.
pickle_name - the name of the pickle to load the data from. Assumes it is in the same data path.
flavor - Tells whether or not we are loading raw data or stats data. Say, 'testdata' or 'cyclestats'
test_id - The test_id needed for uploading data to the database.
'''
def import_BAE_data( file_path, import_from_pickle = False, pickle_name = 'pickle.pkl', flavor = 'testdata', test_id = 1):

  # If we are told to import from a pickle, see if it exists. If it does, import that data from there.
  if import_from_pickle:
    if os.path.exists(file_path + pickle_name) != True:
      print("Pickel path+name does not exisit!")
      return None
    else:
      df_out = pd.read_pickle(file_path + pickle_name)

  # See if the file path exists. If it does, load the raw data in and clean it.
  else:
    if os.path.exists(file_path) != True:
      print("File path does not exisit!")
      return None
    else:
      if flavor == 'testdata':
        df_raw = _import_multiple_csv_data(file_path)
        df_raw = _rename_BAE_data_columns(df_raw) # Rename columns to match our convention 
        df_out = _add_BAE_cell_info_to_df(df_raw)

      elif flavor == 'cyclestats':
        df_raw = _import_multiple_csv_data(file_path)
        df_out = _clean_maccor_cyclestats(df_raw, test_id)
      else: 
        print("Don't know that flavor of test data!")
        return None

  return df_out

'''
This is how we import multiple .csv files
'''
def _import_multiple_csv_data(file_path):

    df_output = pd.DataFrame()
    # r=root, d=directories, f = files
    for r, d, files in os.walk(file_path):

        # We only want to parse files that are CSVs
        files = [ file for file in files if file.endswith( ('.CSV') ) ]
        files.sort()

        for file in files:
            print(file)
            temp_df = pd.read_csv(file_path+file) # Read data from .csv file
            df_output = df_output.append(temp_df, ignore_index = True)

    return df_output


'''
This function renames df columns to meet our naming convention.

Note: Prior to a Dataviewer update in March of 2020, the exported file name would swap the 'Step_index'
and 'Cyc#' heading names. This was then updated as in no longer needed. For legacy data file, set the
flag to "Ture" to swap the naming on these columns 
'''
def _rename_BAE_data_columns(df,flag=False):

    # Rename all columns so that they conform to our standards 
    df.rename(columns={'Date_Time':'datetime'}, inplace=True)
    df.rename(columns={'Test_Time(s)':'test_time_s'}, inplace=True)
    df.rename(columns={'Step_Time(s)':'step_time_s'}, inplace=True)
    df.rename(columns={'Cycle_Index':'cycle'}, inplace=True)
    df.rename(columns={'Step_Index':'step'}, inplace=True)
    df.rename(columns={'TC_Counter1':'tc_counter1'}, inplace=True)
    df.rename(columns={'Voltage(V)':'module_voltage_mv'}, inplace=True)
    df.rename(columns={'Current(A)':'current_ma'}, inplace=True)
    df.rename(columns={'Charge_Capacity(Ah)':'charge_capacity_mah'}, inplace=True)
    df.rename(columns={'Discharge_Capacity(Ah)':'discharge_capacity_mah'}, inplace=True)
    df.rename(columns={'Charge_Energy(Wh)':'charge_energy_mwh'}, inplace=True)
    df.rename(columns={'Discharge_Energy(Wh)':'discharge_energy_mwh'}, inplace=True)
    df.rename(columns={'Aux_Temperature(℃)_1':'aux_temp1_c'}, inplace=True)
    df.rename(columns={'Aux_Temperature(℃)_2':'aux_temp2_c'}, inplace=True)
    df.rename(columns={'ISM_ModuleTemp':'ism_moduletemp_c'}, inplace=True)
    df.rename(columns={'ISM_ModuleTemperature1':'ism_moduletemp1_c'}, inplace=True)
    df.rename(columns={'ISM_ModuleTemperature2':'ism_moduletemp2_c'}, inplace=True)
    df.rename(columns={'ISM_CellVoltages_0':'ism_cell0_voltages_mv'}, inplace=True)
    df.rename(columns={'ISM_CellVoltages_1':'ism_cell1_voltages_mv'}, inplace=True)
    df.rename(columns={'ISM_CellVoltages_2':'ism_cell2_voltages_mv'}, inplace=True)
    df.rename(columns={'ISM_CellVoltages_3':'ism_cell3_voltages_mv'}, inplace=True)
    df.rename(columns={'ISM_CellVoltages_4':'ism_cell4_voltages_mv'}, inplace=True)
    df.rename(columns={'ISM_CellVoltages_5':'ism_cell5_voltages_mv'}, inplace=True)
    df.rename(columns={'ISM_CellVoltages_6':'ism_cell6_voltages_mv'}, inplace=True)
    df.rename(columns={'ISM_CellVoltages_7':'ism_cell7_voltages_mv'}, inplace=True)
    df.rename(columns={'ISM_CellVoltages_8':'ism_cell8_voltages_mv'}, inplace=True)
    df.rename(columns={'ISM_CellVoltages_9':'ism_cell9_voltages_mv'}, inplace=True)
    df.rename(columns={'ISM_CellVoltages_10':'ism_cell10_voltages_mv'}, inplace=True)
    df.rename(columns={'ISM_CellVoltages_11':'ism_cell11_voltages_mv'}, inplace=True)
    df.rename(columns={'OSM_ModChargeCapacity':'osm_modchargecapacity_mah'}, inplace=True)
    df.rename(columns={'ISM_ModuleTotalVoltage':'ism_moduletotalvoltage_mv'}, inplace=True)
    
    # Drop the data we are not using
    if 'ACR' in df.columns:
      df = df.drop(columns=['ACR'])
    if 'Internal_Resistance(Ohm)' in df.columns:
      df = df.drop(columns=['Internal_Resistance(Ohm)'])
    if 'dV/dt(V/s)' in df.columns:
      df = df.drop(columns=['dV/dt(V/s)'])
    if 'Unnamed: 34' in df.columns:
      df = df.drop(columns=['Unnamed: 34'])
    if 'Unnamed: 33' in df.columns:
      df = df.drop(columns=['Unnamed: 33'])
    if 'Unnamed: 35' in df.columns:
      df = df.drop(columns=['Unnamed: 35'])
    if 'Unnamed: 36' in df.columns:
      df = df.drop(columns=['Unnamed: 36'])

    # Get the datetime in the correct format
    timezone = 'PDT'
    df.datetime = pd.to_datetime(df.datetime + ' ' + timezone)
    # Add a column for unix time. Change '1s' to '1ms' to get in ms
    df['unixtime_s'] = (df.datetime.dt.tz_convert('UTC') - pd.Timestamp("1970-01-01",tz='UTC')) // pd.Timedelta('1s')

    return df

'''
This function adds the following df columns to tha passed df:
'cell_maxvoltage_idx' = Index of the maximum cell
'cell_maxvoltage_mv'  = Maximum cell voltage
'cell_minvoltage_idx' = Index of the minimum cell
'cell_minvoltage_mv'  = Minimum cell voltage 
'cell_minmax__delta(mV)' = Maximum diff between any two cells
'''
def _add_BAE_cell_info_to_df(df):

    # Create a sub-dataframe of cell voltages
    Headings = ["ism_cell0_voltages_mv","ism_cell1_voltages_mv",  "ism_cell2_voltages_mv", "ism_cell3_voltages_mv",
                "ism_cell4_voltages_mv","ism_cell5_voltages_mv",  "ism_cell6_voltages_mv", "ism_cell7_voltages_mv",
                "ism_cell8_voltages_mv","ism_cell9_voltages_mv", "ism_cell10_voltages_mv","ism_cell11_voltages_mv"]
    cells_df = df[Headings]
    
    # Create a cell dictionary to translate names to integer cell names
    cell_dict = {
      "ism_cell0_voltages_mv" :  0,
      "ism_cell1_voltages_mv" :  1,
      "ism_cell2_voltages_mv" :  2,
      "ism_cell3_voltages_mv" :  3,
      "ism_cell4_voltages_mv" :  4,
      "ism_cell5_voltages_mv" :  5,
      "ism_cell6_voltages_mv" :  6,
      "ism_cell7_voltages_mv" :  7,
      "ism_cell8_voltages_mv" :  8,
      "ism_cell9_voltages_mv" :  9,
      "ism_cell10_voltages_mv": 10,
      "ism_cell11_voltages_mv": 11,
    }

    # Get the string names of max cells
    temp_max_names = cells_df.idxmax(axis=1)
    temp_min_names = cells_df.idxmin(axis=1)

    # Convert the max cell string names to ints from the cell dictionary
    max_idx = np.zeros(len(temp_max_names),dtype=int)
    min_idx = np.zeros(len(temp_min_names),dtype=int)
    for i in range(0,len(temp_max_names)):
        max_idx[i] = cell_dict[temp_max_names.values[i]]
        min_idx[i] = cell_dict[temp_min_names.values[i]]
     
    # Add the new columns to the data frame
    df['cell_maxvoltage_idx'] = max_idx
    df['cell_maxvoltage_mv']  = cells_df.max(axis=1)
    df['cell_minvoltage_idx'] = min_idx 
    df['cell_minvoltage_mv']  = cells_df.min(axis=1)
    df['cell_minmax_delta_mv'] = (df['cell_maxvoltage_mv']  - df['cell_minvoltage_mv'])

    return df

'''
This is function is used to import a single csv data file

Example call:
data_df = import_functions.import_BAE_data(file_path , data_file_name , data_pickel_name, re_import_data)
'''
def import_BAE_data_old(file_path , file_name , pickle_name , re_import_data = False):
	# If the picle does not already exist or we are told to re-import the data, import it.
	if os.path.exists(file_path + pickle_name) != True or re_import_data:

	    df = pd.read_csv(file_path+file_name) # Read data from .csv file
	    df = _rename_BAE_columns(df) # Rename columns to match our convention
	    df = _add_BAE_cell_info_to_df(df) # Add sepecific cell info to the dataframe 
	    #df.to_pickle(file_path + pickle_name, compression='infer', protocol=4) # Pickle the data
	     
	else: 
	    df = pd.read_pickle(file_path + pickle_name)

	return df

def import_BAE_data_lessold(file_path , import_from_pickle = False, pickle_name = 'pickle.pkl', flavor = 'testdata', test_id = 1):
  # Import the data from the CSVs
  df = _import_multiple_csv_data(file_path)
  # Modify data frame as needed
  df = _rename_BAE_data_columns(df) # Rename columns to match our convention
  df = _add_BAE_cell_info_to_df(df) # Add sepecific cell info to the dataframe 

  return df

# # # BELOW ARE OLD WAYS WE WOULD IMPORT MACCOR DATA

'''
This imports Maccor data files. This is how we used to do it.
'''
def import_maccor_data_old(file_path , file_name , pickle_name = 'temp.pkl', re_import_data = False):
  
  # The pickle does not alrady exist or the reimprot flag is true, import the data
  if os.path.exists(file_path + pickle_name) != True or re_import_data:
      df = pd.read_csv(file_path+file_name,sep='\t', header = 2)  # Read data from .txt is the line the data starts on
      df.to_pickle(file_path + pickle_name, compression='infer', protocol=4) # Pickle the data
  else:
      df = pd.read_pickle(file_path + pickle_name)

  df.rename(columns={'Cyc#':'cycle'}, inplace=True) # Rename to match our analysis function conventions
  df.rename(columns={'Step':'step'}, inplace=True) # Rename to match our analysis function conventions
  df.rename(columns={'TestTime(s)':'test_time_s'}, inplace=True)

  return df

'''
This imports Maccor STATS files
'''
def import_maccor_stats(file_path , file_name ):
  
  df = pd.read_csv(file_path+file_name,sep='\t', header = 8)  # Read data from .txt is the line the data starts on
  df.rename(columns={'Cycle':'cycle'}, inplace=True) # Rename to match our analysis function conventions

  return df

'''
Converts the time series as type string from Maccor stats file and gives us elapsed time in seconds
'''
def _string_to_int(string_series):
    int_series = []
    for x in string_series:
        #the count checks for the digits in days to find the amount that the rest of the string has shifted by
        count = x.index("d") 
        day = ((int(x[2:count]))*86400)
        hours = ((int(x[(count+2):(count+4)]))*3600)
        minutes = ((int(x[count+5:count+7]))*60)
        seconds = (int(x[count+8:count+10]))
        int_series.append((day+hours+minutes+seconds))
    return int_series