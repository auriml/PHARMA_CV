#!/usr/bin/env python
# coding: utf-8

# In[13]:


import pandas as pd
from terminology import ATC, ICD_df
from pathlib import Path
import os
from classes import *


# In[2]:


#generate ATC dictionary file in tagtog format
atc_df = pd.DataFrame({'k': ATC.keys(), 'v': ATC.values()})
atc_df['s'] = atc_df.v.apply(lambda x: '\t'.join(x)  if type(x) == list else '')
atc_df1 = pd.DataFrame(atc_df.v.tolist(), index= atc_df.k)
atc_df1 = atc_df1[atc_df1.index != 'V03AN01'] #remove 'oxigeno' as drug

atc_df1.to_csv('TagTog/Dictionaries/atc.tsv',  header = False, sep = '\t')


# In[3]:


#generate ICD dictionary file in tagtog format
ICD_df.cie10_des = ICD_df.cie10_des.str.strip() 
ICD_df.cie10_cod = ICD_df.cie10_cod.str.strip().str.replace('/', '_') #should be reverted for dictionary lookup 
def f(x): 
    s = ' '.join(x[:7]) 
    return s 
ICD_df.cie10_des = ICD_df.cie10_des.str.split('\s').apply(lambda x: f(x))

ICD_df.to_csv('TagTog/Dictionaries/cie10.tsv', index = False, header = False, sep = '\t')


# #generate tagtog documents (1 document for each patient) using tagblock
# 
# ```meta
# PATIENT:02938240cda555197c41948cfc9c1122128913306d40cb4d045a7456bb885721 is a 41 years old man
# 2020-03-26 00:00:00 State Admission: ER. Total days hospitalization: 11
# 2020-03-26 00:00:00 State Bed: NEUMOLOGIA
# 2020-03-26 14:43:00 VS: Temperatura 36.0
# ```
# 
# ```meta right 
# 2020-03-26 15:33:57 Text CX:
# ```
# 
# ```textbox
# Paciente de 40 anos, acude por 4 dias de iniciar sensacion de dificultad respiratoria, en seguimiento por MAP, el dia de ayer se toma muestra nasofaringea
# ```
# 

# In[ ]:


#tagtog documents

def write_tagtog_docs(area, plist):
    t = Path('TagTog')

    if not os.path.exists(t/area):
        os.makedirs(t/area)

    for p in plist[:]:
        f = open(t/area/f'{p.patient_id}.txt', 'w')
        f.write(f'\nPATIENT: {p}\n')
        for e in p.events:
            if (type(e) in  [Patient, Vs, Diagnosis, Procedure, Oxigen]):
                f.write('```meta no-annotatable \n')
                f.write(str(e)+ '\n')
                f.write('```\n')
            if (type(e) in  [Report]):
                f.write('```meta right no-annotatable \n')
                m = f"{e.start_date} {e.event_type}"
                f.write(str(m)+ '\n')
                f.write('```\n')

                f.write('```textbox\n')
                m = f"{e.event_value}"
                f.write(str(m)+ '\n')
                f.write('```\n')
    f.close() 
    return


# In[ ]:




