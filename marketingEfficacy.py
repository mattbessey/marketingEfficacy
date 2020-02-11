#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from pandas.io import sql
import snowflake.connector
import keyring
import psycopg2 
import time
from datetime import date, timedelta
from scipy import stats
import csv

pd.set_option('display.max_colwidth', 50)
pd.set_option('display.max_columns', 500)

from matplotlib import pyplot as plt
import seaborn as sns
color = sns.color_palette()
#get_ipython().run_line_magic('matplotlib', 'inline')
sns.set_style("darkgrid")


# In[3]:


snowflake_username = 'matthew.bessey@disneystreaming.com'


# In[4]:


ctx = snowflake.connector.connect(authenticator='externalbrowser', 
                                  user=snowflake_username, 
                                  account='disneystreaming.us-east-1')


# In[5]:


# set date parameters for query

subscription_start_date_min = "2019-12-01"
subscription_start_date_max = "2019-12-30" # max of subscription start date
engagement_date = "2020-02-08" # date for which we want to pull engagement behaviors
sample_size = 2000000


# In[39]:


query= """
select 
o.swid
, o.swid_holdout
, e.*
from "DSS_PROD"."DSS"."SFMC_ACCOUNT_SWID_MAP" a
join oneid_combined o on a.swid = o.swid
join "DSS_PROD"."DISNEY_PLUS"."DIM_DISNEY_DAILY_ACCOUNT_ENGAGEMENT" e on a.account_id = e.account_id
where e.LAST_ACCOUNT_SUBSCRIPTION_SIGNUP_DATE >= '{}'
and e.LAST_ACCOUNT_SUBSCRIPTION_SIGNUP_DATE <= '{}'
and e.ds = '{}'
and e.is_pre_launch != 1
limit {};
""".format(subscription_start_date_min, subscription_start_date_max, engagement_date, sample_size)


# In[41]:


# run the query and write to engagement
engagement = pd.read_sql(query,ctx)


# In[42]:


# map columns to lowercase
engagement.columns = engagement.columns.str.lower()


# In[43]:


# create function and apply for mapping of holdout groups on 'swid_holdout'
def holdout_grouping(df):
    if df['swid_holdout'] < 243:
        return "all marketing"
    elif df['swid_holdout'] >= 243 and df['swid_holdout'] < 246:
        return "no onboarding"
    else:
        return "no marketing"
    
engagement['marketing_holdout'] = engagement.apply(holdout_grouping,axis=1)


# In[44]:


#drop rows w/ no entitlement data, rewrite as int
engagement = engagement.drop(engagement[engagement['is_entitled']=='unknown'].index,axis=0)
engagement.is_entitled = engagement.is_entitled.astype(int)


# In[45]:


# remove non-US countries and then country column
countryUS_filter = engagement['account_home_country'] == 'US'
engagementCleaned = engagement[countryUS_filter]
engagementCleaned = engagementCleaned.drop('account_home_country',axis=1)


# In[46]:


# remove unnecessary columns
columnsToRemove = ['swid','swid_holdout','ds','account_id','is_flagged','first_account_subscription_signup_week',
    'last_account_subscription_signup_week','is_entitled_l1','is_entitled_l7','is_entitled_l28','is_entitled_itd',
    'is_pre_launch','is_pre_launch_nltt','account_home_country','subscription_state_upd','subscription_type',
    'total_login_days_l1','total_login_days_l28','total_streams_l1','total_streams_l28','num_streaming_profiles_l1',
    'num_streaming_profiles_l28','num_general_streaming_profiles_l1','num_general_streaming_profiles_l28',
    'num_kids_streaming_profiles_l1','num_kids_streaming_profiles_l28','num_streaming_devices_l1','num_streaming_devices_l28',
    'account_total_stream_days_l1','account_total_stream_days_l28','account_profile_total_stream_days_l1',
    'account_profile_total_stream_days_l28','total_stream_days_general_profiles_l1','total_stream_days_general_profiles_l28',
    'total_stream_days_kids_profiles_l1','total_stream_days_kids_profiles_l28','total_stream_time_ms_l1','total_stream_time_ms_l28',
    'total_stream_time_general_profiles_ms_l1','total_stream_time_general_profiles_ms_l28','total_stream_time_kids_profiles_ms_l1',
    'total_stream_time_kids_profiles_ms_l28','total_stream_time_web_ms_l1','total_stream_time_web_ms_l28','total_stream_time_mobile_ms_l1',
    'total_stream_time_mobile_ms_l28','total_stream_time_connected_tv_ms_l1','total_stream_time_connected_tv_ms_l28',
    'total_stream_time_unknown_ms_l1','total_stream_time_unknown_ms_l28','last_stream_date','account_profile_total_stream_days_l7',
    'account_profile_total_stream_days_itd','total_stream_time_unknown_ms_l7','total_stream_time_unknown_ms_itd'
]

engagementCleaned = engagement.drop(columnsToRemove,axis=1)


# In[47]:


renamedColumns = ['first_signup_date','last_signup_date', 'is_entitled','ttl_login_days_l7', 'ttl_login_days_itd', 'ttl_streams_l7',
    'ttl_streams_itd', 'streaming_profiles_l7','streaming_profiles_itd', 'general_streaming_profiles_l7','general_streaming_profiles_itd', 
    'kids_streaming_profiles_l7','kids_streaming_profiles_itd', 'streaming_devices_l7','streaming_devices_itd', 'ttl_stream_days_l7',
    'ttl_stream_days_itd','ttl_stream_days_general_profiles_l7','ttl_stream_days_general_profiles_itd','ttl_stream_days_kids_profiles_l7',
    'ttl_stream_days_kids_profiles_itd', 'ttl_stream_time_ms_l7','ttl_stream_time_ms_itd', 'ttl_stream_time_general_profiles_ms_l7',
    'ttl_stream_time_general_profiles_ms_itd','ttl_stream_time_kids_profiles_ms_l7','ttl_stream_time_kids_profiles_ms_itd', 
    'ttl_stream_time_web_ms_l7','ttl_stream_time_web_ms_itd', 'ttl_stream_time_mobile_ms_l7','ttl_stream_time_mobile_ms_itd',
    'ttl_stream_time_connected_tv_ms_l7','tl_stream_time_connected_tv_ms_itd', 'days_since_last_stream','marketing_holdout'
]

engagementCleaned.columns = renamedColumns


# In[48]:


# rename for conciseness
engmt = engagementCleaned
del engagementCleaned
del engagement


# In[49]:


# define columns for binary construction
# binary = 1 if action occurred in interval, else 0

binaryConstructionList = [
    'ttl_login_days_l7','ttl_login_days_itd','ttl_streams_l7','ttl_streams_itd','general_streaming_profiles_l7', 
    'general_streaming_profiles_itd','kids_streaming_profiles_l7', 'kids_streaming_profiles_itd','ttl_stream_time_web_ms_l7',
    'ttl_stream_time_web_ms_itd','ttl_stream_time_mobile_ms_l7','ttl_stream_time_mobile_ms_itd', 'ttl_stream_time_connected_tv_ms_l7',
    'tl_stream_time_connected_tv_ms_itd','days_since_last_stream'
]


# In[51]:


# create binary variables _bin
for i in binaryConstructionList:
    engmt[i + '_bin'] = engmt.apply(lambda df:
                                   1 if df[i] > 0
                                   else 0,
                                   axis=1)


# In[52]:


#engmt.groupby('marketing_holdout').mean()


# In[53]:


marketing = engmt[engmt['marketing_holdout'] == 'all marketing']
holdout = engmt[engmt['marketing_holdout'] == 'no marketing']
onboarding_holdout = engmt[engmt['marketing_holdout'] == 'no onboarding']


# In[54]:


test_columns = [
    'is_entitled','ttl_login_days_l7', 'ttl_login_days_itd', 'ttl_streams_l7','ttl_streams_itd',
    'streaming_profiles_l7', 'streaming_profiles_itd','general_streaming_profiles_l7', 'general_streaming_profiles_itd',
    'kids_streaming_profiles_l7', 'kids_streaming_profiles_itd','streaming_devices_l7', 'streaming_devices_itd', 'ttl_stream_days_l7',
    'ttl_stream_days_itd', 'ttl_stream_days_general_profiles_l7','ttl_stream_days_general_profiles_itd','ttl_stream_days_kids_profiles_l7', 
    'ttl_stream_days_kids_profiles_itd','ttl_stream_time_ms_l7', 'ttl_stream_time_ms_itd','ttl_stream_time_general_profiles_ms_l7',
    'ttl_stream_time_general_profiles_ms_itd','ttl_stream_time_kids_profiles_ms_l7','ttl_stream_time_kids_profiles_ms_itd', 
    'ttl_stream_time_web_ms_l7','ttl_stream_time_web_ms_itd', 'ttl_stream_time_mobile_ms_l7','ttl_stream_time_mobile_ms_itd', 
    'ttl_stream_time_connected_tv_ms_l7','tl_stream_time_connected_tv_ms_itd','days_since_last_stream','ttl_login_days_l7_bin', 
    'ttl_login_days_itd_bin','ttl_streams_l7_bin','ttl_streams_itd_bin','general_streaming_profiles_l7_bin',
    'general_streaming_profiles_itd_bin','kids_streaming_profiles_l7_bin','kids_streaming_profiles_itd_bin', 'ttl_stream_time_web_ms_l7_bin',
    'ttl_stream_time_web_ms_itd_bin', 'ttl_stream_time_mobile_ms_l7_bin','ttl_stream_time_mobile_ms_itd_bin',
    'ttl_stream_time_connected_tv_ms_l7_bin','tl_stream_time_connected_tv_ms_itd_bin', 'days_since_last_stream_bin'
]


# In[56]:



p_values = pd.DataFrame(columns=['p','diff','marketing_mean','holdout_mean'])

for i in test_columns:
    try: 
        p1 = stats.ttest_ind(marketing[i], holdout[i])[1]
        descr1 = marketing[i]
        try:
            diff = (marketing[i].mean()/holdout[i].mean()-1)*100
            marketing_mean = marketing[i].mean()
            holdout_mean = holdout[i].mean()
        except (ZeroDivisionError):
            diff = "Undefined"
            marketing_mean = "Undefined"
            holdout_mean = "Undefined"
    except (TypeError,RuntimeWarning): 
        p1 = "Broke!"
    mydict = {'name':i,'p':p1,'diff':diff,'marketing_mean':marketing_mean,'holdout_mean':holdout_mean}
    series = pd.Series(mydict)
    p_values = p_values.append(mydict,ignore_index=True)
    #print("Completed",i)

p_values = p_values.set_index('name')
# In[57]:

filename = "output_" + subscription_start_date_min + "_" + subscription_start_date_max + "_" + str(sample_size) + ".csv"

p_values.to_csv(filename)