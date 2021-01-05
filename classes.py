#!/usr/bin/env python
# coding: utf-8

# In[1]:


from unidecode import unidecode
from utils import *
from terminology import ATC, ICD
import re
import pandas as pd
import numpy as np
import json


# In[4]:


class Event(object):
    def __init__(self, start_date, end_date=None, event_type=None, event_value=None, health_dep =None):
        self.start_date = start_date
        self.end_date = end_date
        self.event_type = event_type
        self.event_value = event_value
        self.health_dep = health_dep
        
    @property #usually same as the class type but may change to differentitate subtyes of Events that share the same class type
    def event_type(self):  
        return self._event_type
    
    @event_type.setter 
    def event_type(self, event_type):
        self._event_type = event_type
        
    
    @property
    def event_value(self): 
        return self._event_value
    
    @event_value.setter 
    def event_value(self, event_value): 
        self._event_value = event_value 
       
    
    
    @property #health department
    def health_dep(self):  
        return self._health_dep
    
    @health_dep.setter 
    def health_dep(self, hd):
        self._health_dep = hd
    
    @property
    def start_date(self): 
        return self._start_date
    
    @start_date.setter 
    def start_date(self, d, source = 'HSJ'):
        d = format_date(d, source)
        self._start_date = d
        
    @property
    def end_date(self): 
        return self._end_date
    
    @end_date.setter 
    def end_date(self, d, source = 'HSJ'):
        d = format_date(d, source)
        self._end_date = d
        

        
    def __repr__(self):
        m = ''
        if self.end_date and self.start_date: 
            total_days = (self.end_date - self.start_date).days
            m = f"{self.start_date} {self.event_type}: {self.event_value}. Total days hospitalization: {total_days }"
        else: 
            m = f"{self.start_date} {self.event_type}: {self.event_value}"
        return m
    
    
    
    


# In[8]:


class Vs(Event):
    def __init__(self, start_date, end_date=None, event_type='VS', vs_name = None, event_value=None, hour = None, health_dep = None):
        self.hour = hour
        self.start_date = start_date
        self.vs_name = vs_name
        self.end_date = end_date
        self.event_type = event_type
        self.event_value = event_value
        self.health_dep = health_dep
    
    @property
    def hour(self): 
        return self._hour
    
    @hour.setter 
    def hour(self, hour):
        self._hour = hour
    
       
    @property
    def start_date(self): 
        return self._start_date
    
    @start_date.setter 
    def start_date(self, d):
        d = format_date(d)
        h = int(self.hour) // 60 
        m = int(self.hour) % 60 
        
        d = d.replace(hour=h, minute=m)
        self._start_date = d
    
    
    @property
    def event_value(self): 
        return self._event_value
    
    @event_value.setter 
    def event_value(self, d): #vs value
        try:
            self._event_value = float(d.replace(',', '.')) 
        except:
            self._event_value = unidecode(d) if (d and type(d) == str) else d
        
    
    @property
    def vs_name(self): 
        return self._vs_name
    
    @vs_name.setter 
    def vs_name(self, t): 
        try:
            t = unidecode(t) if t else None
        except: 
            pass
        self._vs_name = t
        
    def __repr__(self):  
        m = f"{self.start_date} {self.event_type}: {self.vs_name} {self.event_value}"
        return m   
    
    
class Diagnosis(Event):
    def __init__(self, start_date, end_date=None, event_type='Diagnosis', event_value=None, desc = None, health_dep = None):
        super().__init__(start_date, end_date, event_type, event_value, health_dep)
        self.desc = desc
    
    @property
    def desc(self): 
        return self._desc
    
    @desc.setter 
    def desc(self, desc):
        decoded = None
        i = 1
        decoded = ICD.get(self.event_value) 
        while not decoded and len(self.event_value[:-i]) > 0: 
            decoded = ICD.get(self.event_value[:-i]) 
            i += 1
        if not decoded:
            print(self.event_value)
           
        
        self._desc = decoded
    
    def __repr__(self):  
        return f"{self.end_date} {self.event_type}: {self.event_value} {self.desc}"

class Procedure(Event):
    def __init__(self, start_date, end_date=None, event_type='Procedure', event_value=None, desc = None, health_dep = None):
        super().__init__(start_date, end_date, event_type, event_value, health_dep)
        self.desc = desc
        
    @property
    def desc(self): 
        return self._desc
    
    @desc.setter 
    def desc(self, desc):
        decoded = None
        i = 1
        decoded = ICD.get(self.event_value) 
        while not decoded and len(self.event_value[:-i]) > 0: 
            decoded = ICD.get(self.event_value[:-i]) 
            i += 1
        if not decoded:
            print(self.event_value)
           
        
        self._desc = decoded
    
    def __repr__(self):  
        return f"{self.end_date} {self.event_type}: {self.event_value} {self.desc}"
        
class Medication(Event):
    def __init__(self, start_date, end_date=None, event_type='Medication', event_value=None, dose = None, unit = None, freq = None, route = None, atc = None, health_dep = None):
        super().__init__(start_date, end_date, event_type, event_value, health_dep)
        self.event_value = event_value
        self.dose = dose
        self.unit = unit
        self.freq = freq
        self.route = route
        self.atc = atc
        
    
    
            
    @property
    def event_value(self): 
        return self._event_value
    
    @event_value.setter 
    def event_value(self, drug): #principio activo 
        self._event_value = drug
        
    
    @property
    def dose(self): 
        return self._dose
    
    @dose.setter 
    def dose(self, d): 
        if (self.event_type == 'Fluid'): #obtain vol/duration in ml/h  
            #TODO: if not v then extract volumen from 'nemonico' field
            try:
                
                v,h = d[0], d[1]
                v = float(str(v).replace(',', '.')) 
                h = float(str(h).replace(',', '.')) 
                self._dose = v//h
                
            except:
                self._dose = np.nan
            
        else:
            try:
                self._dose = float(str(d).replace(',', '.')) 
            except:
                self._dose = np.nan
        
    
    @property
    def unit(self): 
        return self._unit
    
    @unit.setter 
    def unit(self, u):
        self._unit = u
        
    
    @property
    def freq(self): 
        return self._freq
    
    @freq.setter 
    def freq(self, fre): 
        self._freq = fre
        
    
    @property
    def route(self): 
        return self._route
    
    @route.setter 
    def route(self, rou):   
        self._route = rou
        
    
    
    @property
    def atc(self): 
        return self._atc
    
    @atc.setter 
    def atc(self, atc):
        self._atc = atc
    
    
    def __repr__(self):  
        dose = self.dose if np.isnan(self.dose) == False else ''
        
        unit = self.unit if self.unit else ''
        return f"{self.start_date} {self.event_type}: {self.event_value} {dose } {unit } each: {self.freq} route: {self.route}"

class Oxigen(Event):
    def __init__(self, start_date, end_date=None, event_type='Oxigen', event_value=None, method = None, health_dep = None):
        super().__init__(start_date, end_date, event_type, event_value, health_dep)
        self.method = method
        self.event_value = event_value
        
    @property
    def event_value(self): 
        return self._event_value
    
    @event_value.setter 
    def event_value(self, v):
        try:
            self._event_value = float(v) 
        except:
            self._event_value = np.nan
    
    @property
    def method(self): 
        return self._method
    
    @method.setter 
    def method(self, c):
        self._method = c
    
    def __repr__(self):  
        event_value = self.event_value if np.isnan(self.event_value) == False else ''
        
        method = self.method if self.method else ''
        
        return f"{self.start_date} {self.event_type}: {event_value } {method }"
    
    
class Lab(Event):
    def __init__(self, start_date, end_date=None, event_type='Lab', test_name = None, event_value=None, unit = None, limits = None, health_dep = None, norm_value = None):
        super().__init__(start_date, end_date, event_type, event_value, health_dep)
        self.test_name = test_name
        self.event_value = event_value
        self.unit = unit
        self.limits = limits
        self.norm_value = norm_value
         
        
    
            
    @property
    def norm_value(self): 
        return self._norm_value
    
    @norm_value.setter 
    def norm_value(self, norm_value): #normalized value
        if (norm_value == None) and isinstance(self.limits, list): 
            self._norm_value =  normalize_value( self.event_value, self.limits[0], self.limits[1])
        else:
            self._norm_value = norm_value 
    
            
    
    @property
    def event_value(self): 
        return self._event_value
    
    @event_value.setter 
    def event_value(self, d): #lab value
        try:
            self._event_value = float(d.replace(',', '.')) 
        except:
            self._event_value = d
        
    
    @property
    def test_name(self): 
        return self._test_name
    
    @test_name.setter 
    def test_name(self, t): 
        try:
            t = unidecode(t) if t else None
        except: 
            print(t)
            pass
        self._test_name = t
            
        
    
    @property
    def unit(self): 
        return self._unit
    
    @unit.setter 
    def unit(self, u):
        self._unit = u
        
    
    @property
    def limits(self): 
        return self._limits
    
    @limits.setter 
    def limits(self, limits):
        try: 
            limits = re.sub(r'(\d)-(\d)', '\\1 - \\2', limits ) #replace d-d by d - d as it is mistaked with a negative number 
            numeric_const_pattern = '[-+]? (?: (?: \d* [\.,] \d+ ) | (?: \d+ [\.,]? ) )(?: [Ee] [+-]? \d+ ) ?'
            rx = re.compile(numeric_const_pattern, re.VERBOSE)
            l = rx.findall(limits)
            l = [float(x.replace(',','.')) for x in l]
            self._limits = l if len(l) == 2 else None
        except: 
            self._limits = None
        
        
    def __repr__(self):  
        m = ''
        norm_value = round(self.norm_value, 2) if type(self.norm_value) == float else self.norm_value
        unit = self.unit if self.unit else ''
        if (pd.isna(norm_value) == True):
            m =  f"{self.start_date} {self.event_type}: {self.test_name} {self.event_value} {unit }"
        else:
            m = f"{self.start_date} {self.event_type}: {self.test_name} {self.event_value} {unit } Norm_value: {norm_value}"
        
        return m
    
    
class Report(Event):
    def __init__(self, start_date, end_date=None, event_type='Text', event_value=None, health_dep = None):
        super().__init__(start_date, end_date, event_type, event_value, health_dep)
        self.event_value = event_value
        
    
    
            
    @property
    def event_value(self): 
        return self._event_value
    
    @event_value.setter 
    def event_value(self, t): #text of the event
        t = unidecode(t) if (t and type(t) == str) else t
        self._event_value = t
    
    
    def __repr__(self):
        m = f"{self.start_date} {self.event_type}: {self.event_value}"
        return m
    
    
class Image(Event):
    def __init__(self, start_date, end_date=None, event_type=None, event_value=None, cui_list = None, label_locs = None, health_dep = None):
        self._anon_date = eval(start_date)[0]
        super().__init__(self._anon_date, end_date, event_type, event_value, health_dep)
        #undo days shift
        self.start_date = self.start_date + timedelta(days=10)
        self.event_value = event_value
        self.cui_list = cui_list
        self.label_locs = label_locs
    
    
            
    @property
    def event_value(self): 
        return self._event_value
    
    @event_value.setter 
    def event_value(self, t): #text of the image
        t = unidecode(t) if (t and type(t) == str) else t
        self._event_value = t
        
    @property
    def label_locs(self): 
        return self._label_locs
    
    @label_locs.setter 
    def label_locs(self, label_locs):
        self._label_locs = label_locs
        
    @property
    def cui_list(self): 
        return self._cui_list
    
    @cui_list.setter 
    def cui_list(self, cuis):
        self._cui_list = cuis
        
        
    
    
    
    def __repr__(self):
        m = f"{self.start_date} {self.event_type}: {self.event_value} {self.label_locs}  {self.cui_list}"
        #m = f"{self.start_date} {self.event_type}: {self.event_value} "
        return m

    
class Scale(Event):
    def __init__(self, start_date, end_date=None, scale_name = None, event_type='Scale', event_value=None, health_dep = None):
        super().__init__(start_date, end_date, event_type, event_value, health_dep)
        self.scale_name = scale_name
        self.event_value = event_value
        
    
            
    
    @property
    def event_value(self): 
        return self._event_value
    
    @event_value.setter 
    def event_value(self, d): #Scale value
        try:
            self._event_value = float(d.replace(',', '.')) 
        except:
            self._event_value = d
        
    
    @property
    def scale_name(self): 
        return self._scale_name
    
    @scale_name.setter 
    def scale_name(self, t): 
        try:
            t = unidecode(t) if t else None
        except: 
            print(t)
            pass
        self._scale_name = t
            
        
    
    
        
        
    def __repr__(self):  
        m =  f"{self.start_date} {self.event_type}: {self.scale_name} {self.event_value}"
        return m


# In[9]:


class Patient(object): 
    def __init__(self, patient_id = None, age=None, gender=None, events = None):
        self.patient_id = patient_id
        self.age = age
        self.gender = gender
        self.events = [] if events == None else events
    
    
    @property
    def patient_id(self): 
        return self._patient_id
    
    @patient_id.setter 
    def patient_id(self,patient_id ):
        self._patient_id = patient_id
        
    @property
    def age(self): 
        return self._age
    
    @age.setter 
    def age(self, a): #accepts either age in years or year of birth
        y = format_date(a) #yyyy birth
        
        if y: 
            n = datetime.now()
            a = (n - y).days//365
           
        else: 
            a = float(str(a).replace(',', '.'))
            
        if(a < 0 or a > 120): 
            print(f'OUTLAYER AGE: {a}')
            
        self._age = a
    
    @property
    def gender(self): 
        return self._gender
    
    @gender.setter 
    def gender(self, s): 
        cat = ['man','woman', 'unk_gender']
        if(s in ['Hombre','Mujer']) == False: 
            self._gender = cat[2]
        else:
            self._gender = cat[0] if s == 'Hombre' else cat[1]
        
    @property
    def events(self): 
        return self._events
    
    @events.setter 
    def events(self, events):
        self._events = events
        
    def sort_events(self):
        self.events = sorted(self.events, key=lambda o: o.start_date, reverse=False)
    
    
    
    def __repr__(self):
        return f"{self.patient_id} is a {self.age} years old {self.gender}"
    


# In[ ]:





# In[ ]:




