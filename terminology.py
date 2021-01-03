#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from pathlib import Path


# In[2]:


#load ICD 10 dicts from xml source (not used because low level detailed terms are not included on this source)
import xml.etree.ElementTree as ET
def build_ICD_dict(path, dict_name, type_term = 'Pc'):
    code = 0
    term = 1
    if type_term == 'Dx':
        code = 1
        term = 0
    
    for p in path.ls():  
        tree = ET.parse(p)
        root = tree.getroot()

        for child in root.iter('record'): 
            fields = child.findall('field')
            dict_name[fields[code].text] = fields[term].text
              
    return dict_name 


path_pc = Path('Terminology/Pc')
path_dx = Path('Terminology/Dx')
pc_ICD = {}
dx_ICD = {}
#pc_ICD = build_ICD_dict(path_pc, dict_name = pc_ICD )
#dx_ICD = build_ICD_dict(path_dx, dict_name = dx_ICD, type_term = 'Dx' )


# In[16]:


#Load ICD10 dicts from HSJ source, download file from drive
path_icd = Path('Terminology/cie10.csv')

ICD_df = pd.read_csv(path_icd,  delimiter='\t', encoding='iso-8859-1')
ICD_df = ICD_df.drop_duplicates(subset=['cie10_cod'], keep='last') 
ICD = dict(zip(ICD_df.cie10_cod.str.strip(), ICD_df.cie10_des))


# In[96]:


#Load ATC dict from HSJ source, dowload file from drive
path_atc = Path('Terminology/ATC_BotPlus.tsv')
ATC_df = pd.read_csv(path_atc,  delimiter='\t')

ATC_df = ATC_df.loc[ATC_df.Clase == 'P. Activo uso humano', ['Grupo ATC Nivel 5.1', 'Grupo ATC Nivel 5.2', 'Nombre']]
ATC_df = ATC_df[ATC_df['Grupo ATC Nivel 5.2'] != 'VARIOS']
ATC_df = ATC_df[ATC_df['Nombre'] != 'VARIOS']
ATC = {}
def atc(g,dic):
    atc_5 = g.iloc[0]['Grupo ATC Nivel 5.1'].strip()
    l = list(g['Nombre'].unique())
    l.extend(list(g['Grupo ATC Nivel 5.2'].unique()))
    l = [str.strip(x) for x in l if str(x) != 'nan']
    ATC[atc_5] =l
    return ATC
ATC_df.groupby('Grupo ATC Nivel 5.1').apply(lambda g: atc(g, ATC) )


# In[97]:


#Add trade names associated to ATCs Level 5.2
path_atc = Path('Terminology/ATC_BotPlus_tradenames.tsv')
ATC_trade_df = pd.read_csv(path_atc,  delimiter='\t')


# In[98]:


ATC_trade_df = ATC_trade_df.loc[pd.isna(ATC_trade_df['Grupo ATC Nivel 52']) == False , ['Grupo ATC Nivel 51', 'Grupo ATC Nivel 52', 'Nombre']]
ATC_trade_df = ATC_trade_df[ATC_trade_df['Grupo ATC Nivel 52'] != 'VARIOS']
ATC_trade_df = ATC_trade_df[ATC_trade_df['Nombre'] != 'VARIOS']
ATC_trade_df['TradeName'] = ATC_trade_df.Nombre.str.extract(r'([A-Z]+)') #only keep first token of trade name to keep synonym list short, eg. ATORVASTATINA CINFA, ATORVASTATINA ECF, would be reduced to only one synonym
ATC_trade_df['TradeName'] = ATC_trade_df.TradeName.str.strip()


# In[101]:



def atc_trade(g,dic):
    atc_5 = g.iloc[0]['Grupo ATC Nivel 51'].strip()
    l = list(g['TradeName'].unique())
    l.extend(list(g['Grupo ATC Nivel 52'].unique()))
    l = [str.strip(x) for x in l if (str(x) != 'nan' and len(x) >= 4) and str(x) not in ['VARIOS','ACIDO'] ] #only keep synonym greater than 3 characters
    
    ATC[atc_5] = list(set(l))
    return ATC
ATC_trade_df.groupby('Grupo ATC Nivel 51').apply(lambda g: atc_trade(g, ATC) )


# In[102]:


ATC


# In[ ]:




