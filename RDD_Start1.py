#!/usr/bin/env python
# coding: utf-8

# In[1]:


initRDD=sc.parallelize([3,1,2,5,5])
initRDD.collect()


# In[2]:


stringRDD=sc.parallelize(["apple","text","hello"])
stringRDD.collect()


# In[3]:


def addOne(x):
    return (x+1)
initRDD.map(addOne).collect()


# In[5]:


initRDD.map(lambda x:x+1).collect()


# In[6]:


stringRDD.map(lambda x:"string:"+x).collect()


# In[7]:


initRDD.filter(lambda x:x<3).collect()


# In[8]:


initRDD.filter(lambda x:x==3).collect()


# In[9]:


initRDD.filter(lambda x:1<x and x<5).collect()


# In[10]:


initRDD.filter(lambda x:x>=5 or x<3).collect()


# In[11]:


stringRDD.filter(lambda x:"h" in x).collect()


# In[12]:


initRDD.distinct().collect()


# In[13]:


sRDD=initRDD.randomSplit([0.4,0.6])


# In[14]:


sRDD[0].collect()


# In[15]:


sRDD[1].collect()


# In[16]:


gRDD=initRDD.groupBy(
lambda x:"even" if (x%2==0)
else "odd").collect()


# In[17]:


print(gRDD[0][0],sorted(gRDD[0][1]))


# In[18]:


print(gRDD[1][0],sorted(gRDD[1][1]))


# In[ ]:




