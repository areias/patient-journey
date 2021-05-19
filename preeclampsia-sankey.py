#!/usr/bin/env python
# coding: utf-8

# # Preeclampsia Patient Flow Analysis with Synthea Data 
# 
# Based on:  
# https://github.com/synthetichealth/synthea  
# https://github.com/Neo4jSolutions/patient-journey-model/tree/master/ingest  
# https://github.com/ccdgui/Patient_Flows_Sankey  
#   

# In[205]:


import numpy as np
import re
import datetime


# In[97]:


from py2neo import  Graph, Node
import pandas as pd


# ![](../synthea/schema.png)

# In[1438]:


import importlib
import secrets 


# In[1434]:


db = Graph(scheme="bolt", host=secrets.host, port=secrets.port, secure=False, 
                auth=(secrets.user, secrets.password))


# In[568]:


query="""MATCH (c:Condition {description:"Preeclampsia"}) <-[:HAS_CONDITION]-(e)
return count(e)
"""
df=db.run(query).to_data_frame()
df


# In[1316]:


query="""MATCH (c:Condition {description:"Preeclampsia"}) <-[:HAS_CONDITION]-(e)
with e
MATCH (patient)-[:HAS_ENCOUNTER]-(e)-[:NEXT*]->(e2)-[:HAS_CONDITION|:HAS_DRUG|:HAS_CARE_PLAN|:HAS_ALLERGY|:HAS_PROCEDURE]->(x) 
WHERE e2.date <= ( e.date + duration("P90D") )
OPTIONAL MATCH (e2)-[:HAS_END]->(end)
RETURN labels(x)[0] AS eventType, x.description AS name, 
     e2.date AS startDate,coalesce(end.date, "NA") AS endDate, id(patient) as patient, e2.isEnd as isEnd
     ORDER BY startDate
      """

df=db.run(query).to_data_frame()


# In[1317]:


df.loc[df['patient']==34947]


# In[1318]:


df=df.drop_duplicates()


# In[1319]:


df=df[df['eventType']=="Procedure"]


# In[1320]:


df[df['name']=="Preeclampsia"]


# In[1321]:


df['startDate']=df['startDate'].apply(lambda x: pd.to_datetime(str(x.year)+"-"+str(x.month)+"-"+str(x.day)))


# In[1322]:


for p in df.patient.unique():
    df.loc[df['patient']==p,'delta']=df.loc[df['patient']==p,'startDate']-df.loc[df['patient']==p,'startDate'].shift(1)


# In[1323]:


df['delta']=df['delta'].apply(lambda x: pd.Timedelta(x).days)


# In[1324]:


df['event']=np.nan
for p in df.patient.unique():
    df.loc[df['patient']==p,'event']=np.where(df.loc[df['patient']==p,'delta']>90,1,0)
    df.loc[df['patient']==p,'event']=df.loc[df['patient']==p,'event'].cumsum()


# In[1325]:


df.loc[df['patient']==34947]


# In[1329]:


remove=['Insertion of subcutaneous contraceptive',
'Review of systems (procedure)',
'Extraction of wisdom tooth',
'Insertion of intrauterine contraceptive device',
'Throat culture (procedure)',
'Nasal sinus endoscopy (procedure)',
'Face mask (physical object)',
'Oxygen administration by mask (procedure)',
'Placing subject in prone position (procedure)',
'Plain chest X-ray (procedure)',
'Medication Reconciliation (procedure)',
'Bilateral tubal ligation',
'Movement therapy (regime/therapy)',
'Subcutaneous immunotherapy',
'Spirometry (procedure)',
'Appendectomy',
'Information gathering (procedure)',
'Bone immobilization',
'Cognitive and behavioral therapy (regime/therapy)',
'Brief general examination (procedure)',
'Admission to burn unit', 'Allergy screening test',
'Sputum examination (procedure)',
'Suture open wound',
'Exercise class',
'Measurement of respiratory function (procedure)',
'Kitchen practice']


# In[1330]:


df=df.loc[~df['name'].isin(remove)]


# In[1331]:


df.name.unique()


# In[1332]:


df['event'].unique()


# In[1333]:


for p in df.patient.unique():
    
    for e in df[df['patient']==p]['event'].unique():
        new_date=(df.loc[(df['patient']==p) & (df['event']==e),"startDate"].head(1) - datetime.timedelta(30))

        df=df.append({"eventType":"Procedure",
                                          "name": "Preeclampsia",
                                          "startDate": new_date.item(),
                                          "endDate": np.nan, 
                                          "patient": p,
                                          "isEnd": "False",
                                        "delta": 0,
                                         "event": e},ignore_index=True)


# In[1334]:


df=df.sort_values(["patient","startDate","event"])


# In[1335]:


df.loc[df['patient']==34947]


# In[1336]:


df['idx']=np.nan
for p in df.patient.unique():
    for e in  df.loc[df['patient']==p,"event"].unique():
        df.loc[(df['patient']==p) & (df['event']==e),'idx']=df.loc[(df['patient']==p ) & (df['event']==e)].groupby("startDate").ngroup()


# In[1337]:


df.loc[df['patient']==34947]


# In[1338]:


df.patient.nunique()


# In[1339]:


import itertools
from collections import defaultdict


# In[1367]:


mydict=defaultdict(list)
for p in df.patient.unique():
    for e in df.loc[df['patient']==p]['event'].unique():
        for i in df.idx.unique()[:-1]:
            pid=str(p)+"_"+str(e)
            mydict[pid] += list(itertools.product(
                df[(df['patient']==p) & (df['event']==e) & (df['idx']==i)]['name']+"_"+str(int(i)), 
              df[(df['patient']==p) & (df['event']==e) & (df['idx']==i+1)]['name']+"_"+str(int(i+1))))


# In[1341]:


len(mydict)


# In[1369]:


output_values=list(mydict.values())


# In[1370]:


len(output_values)


# In[1371]:


from collections import Counter


# In[1372]:


frequency = dict(Counter(x for xs in output_values for x in set(xs)))       


# In[1373]:


sankey = {"links": [], "nodes": []}
for i, y in frequency.items():     #links are created first, from items of frequency dictionary    
        link = dict(
            source = str(i[0]),
            target = str(i[1]),
            value = y, 
            )
        sankey["links"].append(link)     
        
        check_node = [link[x] for x in ['source', 'target']]     #nodes derived from links 'source' and 'target' 
        for x in check_node:        #append a new node, only if it does not already exists   
            if not any(d.get('name', None) == x for d in sankey["nodes"]): 
                name = dict(
                    name = x,
                    station = re.sub('[^a-zA-Z]+', '', x),
                    step = re.sub('[^0-9]+', '', x)
                )
                sankey["nodes"].append(name)  


# In[1374]:


sorted_nodes = sorted(sankey['nodes'], key=lambda k: (k['step']))


# In[1375]:


for w, node in enumerate(sorted_nodes):
    node['id'] = w
    node['color'] = 'rgba(31, 119, 180, 0.8)' 


# In[1376]:


len(set([x['station'] for x in sorted_nodes]))


# In[1377]:


cols=['rgb(215,48,39)','rgb(244,109,67)','rgb(253,174,97)','rgb(254,224,144)','rgb(255,255,191)','rgb(224,243,248)','rgb(171,217,233)','rgb(116,173,209)','rgb(69,117,180)',
     'rgb(197,27,125)','rgb(222,119,174)','rgb(241,182,218)','rgb(253,224,239)','rgb(247,247,247)','rgb(230,245,208)','rgb(184,225,134)','rgb(127,188,65)','rgb(77,146,33)',
      'rgb(255,247,236)','rgb(254,232,200)']#,'rgb(253,212,158)','rgb(253,187,132)','rgb(252,141,89)','rgb(239,101,72)','rgb(215,48,31)','rgb(179,0,0)','rgb(127,0,0)',
     #'rgb(178,24,43)','rgb(214,96,77)']#,'rgb(244,165,130)']#,'rgb(253,219,199)','rgb(247,247,247)','rgb(209,229,240)','rgb(146,197,222)','rgb(67,147,195)','rgb(33,102,172)']


# In[1378]:


len(cols)


# In[1379]:


color_dict=[{x[0]:x[1] } for x in list(zip(set([x['station'] for x in sorted_nodes]), cols))]


# In[1380]:


from collections import ChainMap

data = dict(ChainMap(*color_dict))


# In[1381]:


data['Preeclampsia']


# In[1382]:


def id_lookup(node, sorted_list):
    for item in sorted_list: 
        if item['name'] == node['source']:
            return item['id']


# In[1383]:


for d in sankey['links']: 
    d['source_id'] = id_lookup(d, sorted_nodes)    


# In[1384]:


sorted_links = sorted(sankey['links'], key=lambda k: (k['source_id']))  


# In[1385]:


nodes = dict(
            label = [node['name'] for node in sorted_nodes],
            color = [data[node['station']] for node in sorted_nodes]
        )


# In[1386]:


link = dict(
            source = [nodes["label"].index(link['source']) for link in sorted_links ],
            target = [nodes["label"].index(link['target']) for link in sorted_links ],
            value = [link['value'] for link in sorted_links]
        )
            


# In[1387]:


data = dict(nodes=nodes,
        link=link)


# In[1388]:


import plotly as py
from plotly.offline import iplot


# In[1397]:



data_trace = dict(
    type='sankey',
    domain = dict(
      x =  [0,1],
      y =  [0,1]
    ),
    orientation = "h",
    valueformat = ".0f",
    valuesuffix = "Patients",
    node = dict(
      pad = 5,
      thickness = 10,
      line = dict(
        color = "black",
        width = 0.5
      ),
      label =  data["nodes"]["label"],
      color =  data["nodes"]["color"]
    ), 

    link = dict(
      source =  data["link"]["source"],
      target =  data["link"]["target"],
      value =  data["link"]["value"],
      label =  data["nodes"]["label"]
  )   

)

layout =  dict(
    title = "Patient Flow Analysis",
    width=1000,
    height=1000,
    font = dict(
      size = 10   )
)

fig = dict(data=[data_trace], layout=layout)
py.offline.iplot(fig, validate = False)



# In[ ]:





# In[ ]:





# In[ ]:




