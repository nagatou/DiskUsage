# Estimate file space usage in Box.
#
# Copyright Naoyuki Nagatou
# Mode: python3
###

from pathlib import Path
import os
import sys
import re
import gc
import psutil
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import japanize_matplotlib
from mimetypes import guess_type
import glob
import configparser
import datetime

def usage():
   process = psutil.Process(os.getpid())
   return process.memory_info()[1] / float(2 ** 20)

def read_chunks(entry):
   if (os.name != 'nt'):
      minetype = 'text/csv'
   else:
      minetype = 'application/vnd.ms-excel'
   if (guess_type(entry)[0]!=minetype):
      print("No CSV file")
      exit()
   else:
      config = configparser.ConfigParser()
      config.read(os.path.join(os.getcwd(),'estimate_disk_usage.ini'))
      if ('CHUNK' in config) and ('ENCODING' in config):
          return(pd.read_csv(entry,skipinitialspace=True,encoding=config.get('ENCODING','csv_file_encoding'),chunksize=int(config.get('CHUNK','chunksize'))))
      else:
         return(pd.read_csv(entry,skipinitialspace=True,encoding='utf-8',chunksize=100000))

def retrieve_columns(df_csv):
   #tmp = df_csv[['パス','サイズ','作成','最終更新日']]
   tmp = df_csv[['パス','サイズ']]
   del df_csv
   return(tmp.astype(pd.StringDtype()))

def isolate_sec(df_extracted):
   ds = df_extracted['パス']
   df_splited = ds.str.split(pat='/',expand=True,n=4)
   if (len(df_splited.columns) == 5):
      df_sec = df_splited.set_axis(['All','Univ','Dep','Sect','Rest'],axis='columns').astype(pd.StringDtype())
      del ds, df_splited
      tmp = pd.concat([df_sec,df_extracted],axis='columns',ignore_index=False)
      del df_sec, df_extracted
      return(tmp)
   else:
      print("Invalid the number of columns")
      exit(1)
      
def exchange_size_unit(df_isolated):
   ds_size = df_isolated['サイズ']
   ds_size.rename('Size(byte)',inplace=True)
   extract = lambda i: float(re.search(r'\d+[\.\d]*',i).group() if re.search(r'\d+[\.\d]*',i)!=None else exit())
   ds_byte = ds_size.map(lambda size: extract(size)*1000000000000 if ('TB' in size)
                         else (extract(size)*1000000000 if ('GB' in size)
                            else (extract(size)*1000000 if ('MB' in size)
                              else (extract(size)*1000 if ('KB' in size)
                                 else extract(size)))),na_action='ignore')
   del ds_size
   df_with_size = pd.concat([ds_byte,df_isolated],axis='columns',ignore_index=False)
   del df_isolated, ds_byte
   return(df_with_size)

def disk_usage(df,key):
   df_usage = df.groupby(by=key).sum(numeric_only=True)
   del df
   return(df_usage)

def aggregate_by_dep(df_usage,df_dep):
   tmp = df_usage.assign(Dep=np.nan).astype({'Dep':pd.StringDtype()})
   ds_dept = []
   for index,row in tmp.iterrows():
     try:
        ds_dept.append(df_dep.loc[index,'課レベル'])
     except KeyError:
        ds_dept.append("その他")
     except IndexingError:
        print("No exist ", index)
        exit(1)
   tmp['Dep'] = ds_dept
   del df_usage, df_dep
   df_usage_by_dept = tmp.groupby(by='Dep').sum(numeric_only=True)
   return(df_usage_by_dept)

def output_xls(sheets,entry='Box_DiskUsage'):
   config = configparser.ConfigParser()
   config.read(os.path.join(os.getcwd(),'estimate_disk_usage.ini'))
 
   now = datetime.datetime.now()
   if (os.name != 'nt'):
      plt.close('all')
      plt.figure()
      usage_sheet.plot(kind='bar',y='Size(byte)')
      plt.savefig("./Box_DiskUsage_"+now.strftime('%Y%m%d_%H%M%S')+".pdf",bbox_inches='tight')
   else:
      plt.close('all')
      plt.figure()
      usage_sheet.plot(kind='bar',y='Size(byte)')
      plt.savefig("./Box_DiskUsage_"+now.strftime('%Y%m%d_%H%M%S')+".pdf",bbox_inches='tight')

   if 'PATH' in config:
      save_path = config.get('PATH','save_path')
      save_file = save_path+config.get('PATH','save_file_prefix')+now.strftime('%Y%m%d_%H%M%S')+".xlsx"
      if (os.path.exists(save_file)):
         os.remove(save_file)
      with pd.ExcelWriter(save_file,engine='xlsxwriter') as writer:
         for name, sheet in sheets:
            sheet.to_excel(writer,sheet_name=name)
   else:
      save_path=Path('./')
      save_file=save_path/(str(entry.split('.')[0])+"_"+now.strftime('%Y%m%d_%H%M%S')+".xlsx")
      if (os.path.exists(save_file)):
         os.remove(save_file)
      with pd.ExcelWriter(save_file,engine='xlsxwriter') as writer:
         for name, sheet in sheets:
            sheet.to_excel(writer,sheet_name=name)
   return

def fill_sect(df):
   df.fillna(method='ffill',inplace=True)
   return

def get_correspondence_tbl(entry):
   return(pd.read_excel(entry).astype(pd.StringDtype()).set_index('個別フォルダ'))

def csv_file_list():
   config = configparser.ConfigParser()
   config.read(os.path.join(os.getcwd(),'estimate_disk_usage.ini'))
   if 'PATH' in config:
      path = config.get('PATH','csv_file_path')
      prefix = config.get('PATH','csv_file_prefix')
   else:
      path = 'csv_files'
      prefix = '/folder_and_file_tree'
   return(glob.glob(path+prefix+'*.csv',recursive=True))

if __name__ == "__main__":
   if (len(sys.argv)==1):
      config = configparser.ConfigParser()
      config.read(os.path.join(os.getcwd(),'estimate_disk_usage.ini'))
      sg_file = config.get('PATH','sec-group_file')
      if (os.path.exists(sg_file)):
         df_dep_and_sec = get_correspondence_tbl(sg_file)
         fill_sect(df_dep_and_sec)
      else:
         print("No a correspondence table")
         quit(1)
 
      usage = []
      sect = []
      file = []
      print('Progress:',end=" ",flush=True)
      for csv_file in csv_file_list():
         for chunk in read_chunks(csv_file):
            df_isolated = isolate_sec(exchange_size_unit(retrieve_columns(chunk)))
            agg_sect = disk_usage(df_isolated,'Sect')
            usage.append(aggregate_by_dep(disk_usage(df_isolated,'Sect'),df_dep_and_sec).astype({'Size(byte)':float}))
            sect.append(agg_sect)
            file.append(df_isolated)
            gc.collect()
            print('.',end='',flush=True)
      if (len(usage) == 0):
         print("No data")
         quit(1)
      usage_sheet = disk_usage(pd.concat(usage),'Dep')
      usage_sheet_sect = disk_usage(pd.concat(sect),'Sect')
      usage_sheet_file = pd.concat(file)
      
      #output_xls([["sheet1",usage_sheet_file],["グループごとのサマリー",usage_sheet_sect],["課単位でソートしたい時",usage_sheet],["課とグループの対応",df_dep_and_sec]])
      output_xls([["Group Summary",usage_sheet_sect],["Sort by Dep.",usage_sheet],["Dep. and gropu",df_dep_and_sec]])
   else:
      print("Invalid argments")
