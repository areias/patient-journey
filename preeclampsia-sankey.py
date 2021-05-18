#!/usr/bin/env python
# coding: utf-8

# In[205]:


import numpy as np
import re


# In[97]:


from py2neo import  Graph, Node
import pandas as pd


# ![](../synthea/schema.png)

# In[4]:


db = Graph(scheme="bolt", host="northwestern.neo4j.kineviz.com", port=7713, secure=False, 
                auth=('neo4j', 'northwesternAtNeo4j'))


# In[143]:


query="""MATCH (d:Patient)
return id(d)
limit 5"""

df=db.run(query).to_data_frame()


# In[144]:


df


# In[134]:


query="""MATCH (c:Condition {description:"Preeclampsia"}) <-[:HAS_CONDITION]-(e)
MATCH (patient)-[:HAS_ENCOUNTER]-(e)-[:NEXT*]->(e2)-[:HAS_CONDITION|:HAS_DRUG|:HAS_CARE_PLAN|:HAS_ALLERGY|:HAS_PROCEDURE]->(x) 
WHERE e2.date <= ( e.date + duration("P90D") )
OPTIONAL MATCH (e2)-[:HAS_END]->(end)
RETURN labels(x)[0] AS eventType, x.description AS name, 
     e2.date AS startDate,coalesce(end.date, "NA") AS endDate, id(patient) as patient, e2.isEnd as isEnd
     ORDER BY startDate
      """

df=db.run(query).to_data_frame()


# ![](one-patient-journey.png)

# In[135]:


df=df.drop_duplicates()


# In[136]:


df[df['patient']==34946]


# In[143]:


b=df[(df['patient']==34946) &(df['eventType']=="Procedure")]
b['idx'] = b.groupby(['startDate']).ngroup()


# In[144]:


b


# ![](patient-journey-2.png)

# In[138]:


df[(df['patient']==34133) & (df['eventType']=="Procedure")]


# In[139]:


a=df[(df['patient']==34133) & (df['eventType']=="Procedure")]


# In[140]:


a['idx'] = a.groupby(['startDate']).ngroup()


# In[141]:


a


# In[153]:


d=df[(df['patient']==1013) & (df['eventType']=="Procedure")]
d['idx'] = d.groupby(['startDate']).ngroup()


# In[154]:


d


# In[155]:


c=a.append(b)
c=c.append(d)


# In[161]:


#dictionary where 

c[(c['patient']==34133) & (c['idx']==0)]


# In[162]:


c[(c['patient']==34133) & (c['idx']==1)]


# In[179]:


import itertools
from collections import defaultdict


# In[181]:


mydict=defaultdict(list)
for p in c.patient.unique():
    for i in c.idx.unique()[:-1]:
        mydict[p] += list(itertools.product(
            c[(c['patient']==p) & (c['idx']==i)]['name']+"_"+str(i), 
          c[(c['patient']==p) & (c['idx']==i+1)]['name']+"_"+str(i+1)))


# In[182]:


mydict


# In[187]:


output_values=list(mydict.values())


# In[189]:


from collections import Counter


# In[190]:


frequency = dict(Counter(x for xs in output_values for x in set(xs)))       


# In[191]:


frequency


# In[208]:


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


# In[209]:


sankey


# In[210]:


sorted_nodes = sorted(sankey['nodes'], key=lambda k: (k['step']))


# In[211]:


sorted_nodes


# In[212]:


for w, node in enumerate(sorted_nodes):
    node['id'] = w
    node['color'] = 'rgba(31, 119, 180, 0.8)' 


# In[213]:


sorted_nodes


# In[214]:


def id_lookup(node, sorted_list):
    for item in sorted_list: 
        if item['name'] == node['source']:
            return item['id']


# In[215]:


for d in sankey['links']: 
    d['source_id'] = id_lookup(d, sorted_nodes)    


# In[216]:


d


# In[217]:


sorted_links = sorted(sankey['links'], key=lambda k: (k['source_id']))  


# In[218]:


sorted_links


# In[221]:


nodes = dict(
            label = [node['name'] for node in sorted_nodes],
            color = [node['color'] for node in sorted_nodes]
        )


# In[222]:


link = dict(
            source = [nodes["label"].index(link['source']) for link in sorted_links ],
            target = [nodes["label"].index(link['target']) for link in sorted_links ],
            value = [link['value'] for link in sorted_links]
        )
            


# In[224]:


data = dict(nodes=nodes,
        link=link)


# In[225]:


data


# In[239]:


import plotly as py
from plotly.offline import iplot


# In[240]:



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
      pad = 15,
      thickness = 15,
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
    font = dict(
      size = 10
    )
)

fig = dict(data=[data_trace], layout=layout)
py.offline.iplot(fig, validate = False)


# In[ ]:




