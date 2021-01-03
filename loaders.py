#!/usr/bin/env python
# coding: utf-8

# In[1]:


from utils import *
from terminology import ATC, ICD
from classes import *
from tagtog import write_tagtog_docs

import pandas as pd
import re
import os
import json
import jsonpickle
from unidecode import unidecode
from pathlib import Path
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 170)
pd.set_option('display.max_colwidth', 100000)
import numpy as np

import sklearn.metrics as skm
from concurrent.futures import ProcessPoolExecutor


# ## Create and save a single big datatable from a dictionary of patients

# In[2]:


def patient_to_table(item):
    key, p = item
    area = key.split('_')[0]
    events = p.events
    p1 = p.__dict__.copy()
    del p1['_events']
    p1['_health_dep'] = '01'
    d = dt.Frame([{**e.__dict__, **p1} for e in events])
    d[:, [f._start_date,f._end_date,f._limits] ]= d[:, [dt.str32(f._start_date),dt.str32(f._end_date),dt.str32(f._limits)]]
    return d

def create_big_table(pdict, file = 'bigTable'):
    bigTable = dt.Frame()
    with ProcessPoolExecutor() as executor:
            for p_table in  executor.map(patient_to_table, pdict.items()):
                bigTable = dt.rbind(bigTable, p_table, force = True)
    bigTable.to_jay(f"out_EDA/{file}.jay")            
    return bigTable


# ## Load patient objects from json files and return a dictionary of patients

# In[5]:


def paralleliz(dh):
    print(dh)
    pdict = {}
    area = dh.with_suffix("").name
    for p in dh.iterdir():
        with open(p) as f:
            frozen = f.read()
            p = jsonpickle.decode(frozen)
            pdict[f'{area}_{p.patient_id}'] = p
    return pdict

def load_objects(): #read from json into patient objects and return a dictionary of patients
    pdict = {}
    objects_path = Path('out_Objects')
    with ProcessPoolExecutor() as executor:
            for dh, d in zip(objects_path.iterdir(), executor.map(paralleliz, objects_path.iterdir())):
                pdict = {**pdict, **d}
    return pdict


# ## Load source tables, create patient objects, return a dictionary of patients and serialize patients to json files

# In[3]:


# define loaders functions one for each table as provided form source 
def status(g): #table EVOLUCION
    x = g.iloc[0]
    p = Patient(patient_id = x.paciente, age = x.paciente_fnac, gender = x.paciente_sexo)  
    
    elist = g.apply(lambda e: Event(e.fecha_ingreso, 
                                    end_date=e.fecha_alta , event_type='State Admission', event_value='ER'
                                   ), axis = 1).values
    p.events.extend(elist)
    
    elist = [Event(e.fecha_ingreso_desde_urgencias, 
                                    end_date=e.tras_1_fecha , event_type='State Bed', event_value=e.servicio_ingreso
                                   ) for i,e in g.iterrows() if len(str(e.fecha_ingreso_desde_urgencias))>10]
    p.events.extend(elist)
        
    
    elist = [Event(e.tras_1_fecha, 
                   end_date=e.tras_2_fecha ,event_type='State Bed', event_value=e.tras_1_servicio, 
                  ) for i,e in g.iterrows() if len(str(e.tras_1_fecha))>10]
    p.events.extend(elist)
    elist = [Event(e.tras_2_fecha, 
                   end_date=e.tras_3_fecha ,event_type='State Bed', event_value=e.tras_2_servicio,
                  ) for i,e in g.iterrows() if len(str(e.tras_2_fecha))>10]
    p.events.extend(elist)
    elist = [Event(e.tras_3_fecha, 
                   end_date=e.tras_4_fecha ,event_type='State Bed', event_value=e.tras_3_servicio, 
                  ) for i,e in g.iterrows() if len(str(e.tras_3_fecha))>10]
    p.events.extend(elist)
    elist = [Event(e.tras_4_fecha, 
                   end_date=e.tras_5_fecha , event_type='State Bed', event_value=e.tras_4_servicio, 
                  ) for i,e in g.iterrows() if len(str(e.tras_4_fecha))>10]
    p.events.extend(elist)
    
    elist = [Event(e.tras_5_fecha, 
                   end_date=e.tras_6_fecha ,event_type='State Bed', event_value=e.tras_5_servicio, 
                  ) for i,e in g.iterrows() if len(str(e.tras_5_fecha))>10]
    p.events.extend(elist)
    
    elist = [Event(e.fecha_alta, 
                   end_date=None , event_type='State Discharge', event_value=e.destino_alta
                  ) for i,e in g.iterrows() if len(str(e.fecha_alta))>10]
    p.events.extend(elist)
    
    elist = [Diagnosis(e.fecha_alta, 
                   end_date=e.fecha_alta ,  event_value=dx
                  ) for i,e in g.iterrows() for dx in re.findall( r'\[(.*?)\]', str(e.diagnosticos)) if len(str(e.diagnosticos))>4]
    p.events.extend(elist)
    
    
    elist = [Procedure(e.fecha_alta, 
                   end_date=e.fecha_alta ,  event_value=pc
                  ) for i,e in g.iterrows() for pc in re.findall( r'\[(.*?)\]', str(e.procedimientos)) if len(str(e.procedimientos))>4]
    p.events.extend(elist)
    
    
    p.sort_events()
    
    return p  


def load_patients(dfs, filter_patients = None):
    d = dfs['DATOS_EVOLUCION_PACIENTES']
    patients = None
    if filter_patients:
        d = d.loc[d.paciente.isin(filter_patients)]
        
        patients = d.groupby('paciente').apply(lambda x: status(x) )
    else:
        patients = d.groupby('paciente').apply(lambda x: status(x) )
   
    return patients.values

def image(g,pdict):
    x = str(g.iloc[0].PatientID)
    if x not in anon.MIDS_ID.values: 
        return
    sip = anon.loc[anon.MIDS_ID == x, 'SIPANON'].values[0]
    #print(f'missing patients in datos_evolucion_pacientes but found in images ')
    if ((sip in pdict.keys()) == False):
        return
    
    else:
        x = sip
        p = pdict[x]
        g.loc[pd.isna(g.LabelsLocalizationsBySentence) == True, ['LabelsLocalizationsBySentence']] = None  
        elist = [Image(e['Study Date'], 
                        end_date=None , event_type=f"{e.Modality} {e['Body Part Examined']}" , label_locs = e.LabelsLocalizationsBySentence, event_value=e.Report, cui_list = e.labelCUIS
                       ) for i,e in g.iterrows() if len(str(e['Study Date']))>8 ]
        p.events.extend(elist)
        
        p.sort_events()
        
        pdict[x] = p
    return pdict

def scale(g,pdict ):
    x = g.iloc[0].paciente
    #print(f'missing patients in datos_evolucion_pacientes but found in PRUEBAS_COVID ')
    if (x in pdict.keys()) == False:
        print(x)
    else:
        p = pdict[x]
        elist = [Scale(e.fecha, 
                        end_date=None , event_type='Scale', scale_name = e.escala, event_value=e.valor
                       ) for i,e in g.iterrows() if (len(str(e.fecha))>10 & (pd.isnull(e.escala) == False))]
        p.events.extend(elist)
        
        
        
    
        p.sort_events()
        
        pdict[x] = p
    return pdict

def vs(g,pdict ):
    x = g.iloc[0].paciente
    #print(f'missing patients in datos_evolucion_pacientes but found in PRUEBAS_COVID ')
    if (x in pdict.keys()) == False:
        print(x)
    else:
        p = pdict[x]
        elist = [Vs(e.fecha, 
                        end_date=None , event_type='VS', vs_name = e.constante, event_value=e.valor, hour = e.hora
                       ) for i,e in g.iterrows() if (len(str(e.fecha))>10 & (pd.isnull(e.constante) == False))]
        p.events.extend(elist)
        
        
        
    
        p.sort_events()
        
        pdict[x] = p
    return pdict

def report(g,pdict, report_type, report_field ):
    x = g.iloc[0].paciente
    #print(f'missing patients in datos_evolucion_pacientes but found in DATOS_INFORMES_ANAMNESIS')
    if (x in pdict.keys()) == False:
        print(x)
    else:
        g = g.loc[g[report_field].isna() == False]
        p = pdict[x]
        elist = [Report(e.fecha_informe, event_type=report_type, event_value=e[report_field]
                       ) for i,e in g.iterrows() if (len(str(e.fecha_informe))>10 & (e[report_field] != False) & (type(e[report_field]) == str))]
        p.events.extend(elist)
        
    
        p.sort_events()
        
        pdict[x] = p
    return pdict
        
def lab_covid(g,pdict ):
    x = g.iloc[0].paciente
    #print(f'missing patients in datos_evolucion_pacientes but found in PRUEBAS_COVID ')
    if (x in pdict.keys()) == False:
        print(x)
    else:
        p = pdict[x]
        elist = [Lab(e.fecha_prueba, 
                        end_date=None , event_type='Lab Sars-cov-2', test_name = e.prueba, event_value=e.resultado
                       ) for i,e in g.iterrows() if len(str(e.fecha_prueba))>10]
        p.events.extend(elist)
        
        
        
    
        p.sort_events()
        
        pdict[x] = p
    return pdict

def lab(g,pdict ):
    x = g.iloc[0].paciente
    #print(f'missing patients in datos_evolucion_pacientes but found in DATOS_LABORATORIO')
    if (x in pdict.keys()) == False:
        print(x)
    else:
        p = pdict[x]
        elist = [Lab(e.fecha_prueba, 
                        end_date=None , event_type='Lab', test_name = e.prueba_lab, event_value=e.resultado_lab,  unit = e.UNIDAD, limits=e.REFERENCIA
                       ) for i,e in g.iterrows() if len(str(e.fecha_prueba))>10]
        p.events.extend(elist)
        
        
        
    
        p.sort_events()
        
        pdict[x] = p
    return pdict
        
def fluid(g,pdict ):
    x = g.iloc[0].paciente

    #print(f'missing patients in datos_evolucion_pacientes but found in FLUIDOTERAPIA_OC')
    if (x in pdict.keys()) == False:
        print(x)
    else:
        p = pdict[x]
        elist = [Medication(e.fecha_inicio, 
                        end_date=e.fecha_fin , event_type='Fluid', event_value=e.principio_activo, dose = (e.volumen,e.duracion), unit = 'ml/h', atc=e.atc 
                       ) for i,e in g.iterrows() if (len(str(e.fecha_inicio))>10) & (e.valida_farmacia == 1)]
        p.events.extend(elist)
        
        
        p.sort_events()
        
        pdict[x] = p
    return pdict

def gas(g,pdict ):
    x = g.iloc[0].paciente
    #print(f'missing patients in datos_evolucion_pacientes but found in gasoterapia_oc')
    if (x in pdict.keys()) == False:
        print(x)
    else:
        p = pdict[x]
        elist = [Oxigen(e.fecha_inicio, 
                        end_date=e.fecha_fin , event_type='Oxigen', event_value=e.fio2, method=e.metodo
                       ) for i,e in g.iterrows() if len(str(e.fecha_inicio))>10]
        p.events.extend(elist)
        
    
        p.sort_events()
    return pdict

def drug(g,pdict ):
    x = g.iloc[0].paciente
    #print(f'missing patients in datos_evolucion_pacientes but found in MEDICACION')
    if (x in pdict.keys()) == False:
        print(x)
    else:
        p = pdict[x]
        elist = [Medication(e.fecha_administracion_paciente, 
                        end_date=e.fecha_fin , event_type='Medication', event_value=e.principio_activo, dose = e.dosis, unit = e.unidad_medida, freq = e.frecuencia, route = e.forma_administracion, atc=e.atc 
                       ) for i,e in g.iterrows() if len(str(e.fecha_administracion_paciente))>10]
        p.events.extend(elist)
        
        
        
    
        p.sort_events()
        
        pdict[x] = p
    return pdict


# In[4]:


path = Path('datos_7') #path to data tables as provided from source
print(list(path.iterdir()))


# In[8]:


dfs = {}
plist = []  
pdict = {}
load_tables = False #Read source tables into objects  (see classes representing a patient)
write = False #write or update a txt file by health department wih human-readible patients represented as events ordered by time
tagtog = False #write or update annotable files in TagTog format
profile = True #profile source tables generating html reports
serialize = False #serialize patient objects into json files (see classes representing a patient)

areas = ['08','06','05','18','01','17','19']
areas = ['01']

if load_tables:
    for dh in path.iterdir(): #iterate health departments
        area = dh.with_suffix("").name
        if area in areas:
            dfs = {}
            plist = []  
            pdict = {}
            for f in dh.iterdir():
                pd_name = f'{f.with_suffix("").name}'
                pd_name = re.sub(r"_\d+$", "", pd_name) #table name remove 'area de salud - number '
                pd_name = re.sub(r"_OC$", "", pd_name) #table name remove 'orion clinic - OC'
                print(pd_name)
                df = None
                try:
                    df = pd.read_csv(f,  delimiter='\t')
                except: 
                    df = pd.read_csv(f, encoding='iso-8859-1', delimiter='\t')

                print(df.columns)
                dfs[pd_name] = df

            if profile:
                from pandas_profiling import ProfileReport
                #profile = ProfileReport(df, minimal=True, pool_size = 32)
                for key in dfs.keys():
                    try:
                        profile = ProfileReport(dfs[key],  pool_size = 32)
                        profile.to_file(f'out_EDA/{area}_{key}.html')
                    except: 
                        print(f'skipped {key}')

            print(f'AREA:{area}. Starting to create patient dictionary .....')
            plist = load_patients(dfs)

            pdict = {p.patient_id:p for p in plist}
            print(f'N Patients:{len(pdict)}')

            print('update patient dict with image tests')
            anon = pd.read_csv(path/'ANONIMIZACION.txt', sep = '\t')
            anon.MIDS_ID = anon.MIDS_ID.str.replace('_','-')
            # Rx-thorax-automatic-captioning/COVID_QC$ vi COVID19_POSI_v2.0.csv to obtain study_date and session
            # Labels_covid_12.csv to obtain text, labels and cuis 
            path_rx = Path('../Rx-thorax-automatic-captioning')
            cq1 = pd.read_csv(path_rx /'COVID_QC/COVID19_POSI_v2.0.csv')[['Subject','Session','Study Date','Modality', 'Body Part Examined']]
            cq2 = pd.read_csv(path_rx /'COVID_QC/COVID19_POSI_v1.0.csv')[['Subject','Session','Study Date','Modality', 'Body Part Examined']]
            img_cq = pd.concat([cq1,cq2])
            img_labels = pd.read_csv(path_rx /'Labels_covid_12.csv')[['PatientID','ReportID','Report','Labels', 'LabelsLocalizationsBySentence', 'labelCUIS', 'LocalizationsCUIS']]
            print(img_labels.columns)
            mer = img_cq.merge(img_labels, how = 'inner', left_on = ['Session'], right_on = ['ReportID'], indicator = True, ).drop_duplicates('ReportID')
            mer.groupby('PatientID').apply(lambda x: image(x, pdict) )

            print('update patient dict with Scales')
            dfs['DATOS_ESCALAS'].groupby('paciente').apply(lambda x: scale(x, pdict) ).values

            print('update patient dict with VS')
            dfs['DATOS_CONSTANTES'].groupby('paciente').apply(lambda x: vs(x, pdict) ).values


            print('update patient dict with reports')
            dfs['DATOS_INFORMES_ANAMNESIS'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text CX', 'enfermedad_actual') ).values
            dfs['DATOS_INFORMES_ANAMNESIS'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text PE', 'exploracion_fisica') ).values
            dfs['DATOS_INFORMES_ANAMNESIS'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text MH', 'antecedentes') ).values
            dfs['DATOS_INFORMES_ANAMNESIS'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text sDX', 'COD_sospecha_diagnostica') ).values
            dfs['DATOS_INFORMES_ANAMNESIS'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text PLAN', 'plan') ).values

            dfs['DATOS_INFORMES_ALTA_URGENCIAS'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text CX', 'enfermedad_actual') ).values
            dfs['DATOS_INFORMES_ALTA_URGENCIAS'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text PE', 'exploracion_fisica') ).values
            dfs['DATOS_INFORMES_ALTA_URGENCIAS'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text MH', 'antecedentes') ).values
            dfs['DATOS_INFORMES_ALTA_URGENCIAS'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text EV', 'evolucion') ).values
            dfs['DATOS_INFORMES_ALTA_URGENCIAS'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text 1DX', 'COD_diag_principal') ).values
            dfs['DATOS_INFORMES_ALTA_URGENCIAS'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text 2DX', 'COD_diag_secundario') ).values
            dfs['DATOS_INFORMES_ALTA_URGENCIAS'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text PX', 'COD_proc_diag_terap') ).values
            dfs['DATOS_INFORMES_ALTA_URGENCIAS'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text QX', 'COD_proc_quir') ).values
            dfs['DATOS_INFORMES_ALTA_URGENCIAS'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text TX', 'tratamiento') ).values
            dfs['DATOS_INFORMES_ALTA_URGENCIAS'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text RE', 'recomendaciones') ).values



            dfs['DATOS_INFORMES_ALTA_HOSPITALIZACION'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text CX', 'enfermedad_actual') ).values
            dfs['DATOS_INFORMES_ALTA_HOSPITALIZACION'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text PE', 'exploracion_fisica') ).values
            dfs['DATOS_INFORMES_ALTA_HOSPITALIZACION'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text MH', 'antecedentes') ).values
            dfs['DATOS_INFORMES_ALTA_HOSPITALIZACION'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text EV', 'evolucion') ).values
            dfs['DATOS_INFORMES_ALTA_HOSPITALIZACION'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text 1DX', 'COD_diag_principal') ).values
            dfs['DATOS_INFORMES_ALTA_HOSPITALIZACION'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text 2DX', 'COD_diag_secundario') ).values
            dfs['DATOS_INFORMES_ALTA_HOSPITALIZACION'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text PX', 'COD_proc_diag_terap') ).values
            dfs['DATOS_INFORMES_ALTA_HOSPITALIZACION'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text QX', 'COD_proc_quir') ).values
            dfs['DATOS_INFORMES_ALTA_HOSPITALIZACION'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text TX', 'tratamiento') ).values
            dfs['DATOS_INFORMES_ALTA_HOSPITALIZACION'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text RE', 'recomendaciones') ).values
            dfs['DATOS_INFORMES_ALTA_HOSPITALIZACION'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text Discharge', 'destino_alta') ).values


            dfs['DATOS_INFORMES_NOTAS_MED_EVOLUCION'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text EV', 'seguimiento_actual') ).values
            dfs['DATOS_INFORMES_NOTAS_MED_EVOLUCION'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text sDX', 'COD_sospecha_diagnostica') ).values
            #dfs['DATOS_INFORMES_NOTAS_MED_EVOLUCION'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text TE', 'exploracion_complementaria') ).values
            dfs['DATOS_INFORMES_NOTAS_MED_EVOLUCION'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text PLAN', 'plan') ).values

            dfs['DATOS_INFORMES_CAMBIO_SERVICIO'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text CX', 'enfermedad_actual') ).values
            dfs['DATOS_INFORMES_CAMBIO_SERVICIO'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text PE', 'exploracion_fisica') ).values
            dfs['DATOS_INFORMES_CAMBIO_SERVICIO'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text MH', 'antecedentes') ).values
            dfs['DATOS_INFORMES_CAMBIO_SERVICIO'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text EV', 'evolucion') ).values
            dfs['DATOS_INFORMES_CAMBIO_SERVICIO'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text 1DX', 'COD_diag_principal') ).values
            dfs['DATOS_INFORMES_CAMBIO_SERVICIO'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text 2DX', 'COD_diag_secundario') ).values
            dfs['DATOS_INFORMES_CAMBIO_SERVICIO'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text PX', 'COD_proc_diag_terap') ).values
            dfs['DATOS_INFORMES_CAMBIO_SERVICIO'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text QX', 'COD_proc_quir') ).values
            dfs['DATOS_INFORMES_CAMBIO_SERVICIO'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text TX', 'tratamiento') ).values
            dfs['DATOS_INFORMES_CAMBIO_SERVICIO'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text RE', 'recomendaciones') ).values
            dfs['DATOS_INFORMES_CAMBIO_SERVICIO'].groupby('paciente').apply(lambda x: report(x, pdict, 'Text Discharge', 'destino_alta') ).values


            print('update patient dict with COVID tests')
            dfs['DATOS_MICROBIOLOGIA'].groupby('paciente').apply(lambda x: lab_covid(x, pdict) ).values

            print('update patient dict with lab tests')
            dfs['DATOS_LABORATORIO'].groupby('paciente').apply(lambda x: lab(x, pdict) ).values

            print('update patient dict with fluid therapy')
            dfs['DATOS_FLUIDOTERAPIA'].groupby('paciente').apply(lambda x: fluid(x, pdict) ).values

            print('update patient dict with gas therapy')
            dfs['DATOS_GASOTERAPIA'].groupby('paciente').apply(lambda x: gas(x, pdict) ).values

            print('update patient dict with medication')
            dfs['DATOS_MEDICACION'].groupby('paciente').apply(lambda x: drug(x, pdict) ).values

            if serialize: 
                t = Path('out_Objects')

                if not os.path.exists(t/area):
                    os.makedirs(t/area)

                for p in plist[:]:
                    f = open(t/area/f'{p.patient_id}.json', 'w')
                    #convert to JSON string
                    frozen = jsonpickle.encode(p, indent = 2, unpicklable=True) #unpicklable = False to output clean json (it can only be restored as dicts but not converted to objects)
                    f.write(frozen)
                    f.close() 




            if write:
                f = open(f'out_EDA/{area}_events_by_patient.txt', 'w')
                #f = open('prueba.txt', 'w')
                for p in plist[:]:
                    f.write(f'\nPATIENT: {p}\n')
                    for e in p.events:
                        if (type(e) == Lab) and type(e.norm_value) == float and (e.norm_value <1 and e.norm_value >0):
                            pass
                        else:
                            #print(str(e))
                            f.write(str(e)+ '\n')
                f.close() 

            if tagtog: 
                write_tagtog_docs(area,plist)
            
        
        

