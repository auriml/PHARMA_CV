# PHARMA_CV
Pharma CV is a project to analyze all patients admitted to hospitals from 12 health departments from the Valencian Region from March 2020 to 30 November 2020 with a diagnosis of COVID confirmed by microbiology tests. An objective of the project is predictive modeling of patient outcomes.

Source data once anonymized were provided in tables for demography, admissions and discharge dates from hospital departments,  medication, gasometric tests, fluidotherapy, laboratory tests, covid microbiology tests, medical notes from all encounters including emergency room, hospitalizations, and intensive care units in natural lenguage, vital signs, clinical scales, image reports and destination after hospital discharge.   (To download list of tables ask for drive access)

## Python Modules
The code on this repository is organized in python modules: 

- **Loaders.py**
1. This is the first step: Read source tables, create patient objects, return a dictionary of patients and serialize patients to json files
> PRE-REQUIREMENT: directory with source tables
>> Read source tables into objects  (see classes representing a patient)

>>Write or update a txt file for each health department wih human-readible patients represented as events ordered by time

>> Write or update annotable files in TagTog format

>> Profile source tables generating html reports

>> Serialize patient objects into json files (see classes representing a patient)

> OUT: patient_id.json

2. Load patient objects from json files and return a dictionary of patients as python objects (see classes signatures )
>> PRE-REQUIREMENT: 1º step generated json files


3. Create and save a single big datatable from a dictionary of patients
> PRE-REQUIREMENT: preloaded dictionary of patients

> OUT: A table 'out_EDA/bigTable.jay' with 3M events (rows) from 4.5K patients with all clinical information condensed in 24 columns. 
*('_start_date',
 '_end_date',
 '_event_type',
 '_event_value',
 '_health_dep',
 '_test_name',
 '_unit',
 '_limits',
 '_norm_value',
 '_patient_id',
 '_age',
 '_gender',
 '_anon_date',
 '_cui_list',
 '_label_locs',
 '_hour',
 '_vs_name',
 '_method',
 '_dose',
 '_freq',
 '_route',
 '_atc',
 '_desc',
 '_scale_name')* 

- **Classes.py:**
> Class patient has attributes 'patient_id', 'age', 'gender', 'events=L(Event)'. 

> Each Event has attributes 'start_date', 'end_date', 'event_type', 'event_value', 'health_dep=L(12 health departments)'. All clinical information is modeled as an Event and hence inherits from it. Each type of events expands with their own attributes. Events can be of any of the fhe following types: 
>> State Admission (in hospital)

>> State Bed (any hospital service)

>> State Discharge (out hospital)

>> Image (includes natural text and UMLS_CUIS for medical entities and anatomical locations)

>> Clinical Scale

>> Vital Sign

>> Report (natural text): Subtypes are 
>>> Annanmesis -CX

>>> Medical History -MH

>>> Physical Examination -PE

>>> Evolution -EV

>>> Plan -Plan

>>> Suspect Diagnoses -sDX

>>> Primary and Secondary Diagnoses -1DX -2DX

>>> Procedures -PX

>>> Surgical Procedures -QX

>>> Treatment -TX

>>> Recomendation -RE

>>> Destination at Discharge -Discharge 

>> Laboratory Test

>> Laboratory Covid Test

>> Fluidotherapy

>> Gasotherapy

>> Medication
- **Terminology.py**
>> Drugs: 2.8K drugs identified by ATC level 5.2 with their active substance and different trade/presentation names as synonym

>> Medical Entities: ICD10 
- **Clinical_scales.py**
> Scripts to calculate Charlson and other scales
- **EDA.py**
- **Utils.py**
> Utilities for value normalization, date formatting, etc,.. 
- **TagTog.py**
> Utilities to generate documents for manual annotation as required in TagTog platform.
