#!/usr/bin/env python
# coding: utf-8

# In[1]:


from datetime import datetime, timedelta

    


# In[2]:


def format_date(s, source='HSJ'):
    x = None
    for fmt in ('%Y/%m/%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y/%d/%m %H:%M:%S', '%Y%m%d', '%Y', '%d/%m/%Y'):  #usage example (y - x).days for delta time in days
        try:
            x= datetime.strptime(str(s), fmt)

            return x 
        except:
            
            pass

    #print(f"Failed date {s}") 
    return x



def normalize_value(x,mini, maxi): #x normalized = (x – x minimum) / (x maximum – x minimum)
        x_norm = None
        try: 
            x_norm = (x-mini) / (maxi - mini)
        except: 
            pass
        return x_norm

