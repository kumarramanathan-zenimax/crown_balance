# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### adobe_campaign_eso_vc_metrics_second
# MAGIC **Title of notebook:** adobe_campaign_eso_vc_metrics                 
# MAGIC **Contact:**  Kumar Ramanathan        
# MAGIC **Games:** ESO  
# MAGIC **Notebook dependencies:**  
# MAGIC **Library dependencies:** None    
# MAGIC **Language written:** Python  
# MAGIC **Schedule:** schedule to run once a day at ** TBD**   
# MAGIC **Purpose of code:** The code is to create a csv file for adobe campaign with all eso vc metrics data    
# MAGIC **Idempotent:** Rerunning within the same day will give the same data  
# MAGIC 
# MAGIC *Changelog*
# MAGIC * **20211115:** Created job based on [MKT-509] https://zenimaxbi.atlassian.net/browse/MKT-509

# COMMAND ----------

# MAGIC %run "/Production/Adobe_Marketing/Adobe SFTP and Encryption"

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table eso.virtual_currencydatabaseworker;
# MAGIC refresh table eso_reporting.eso_crowns_subs_transactions;

# COMMAND ----------

#WHICH_RUN = dbutils.widgets.get("whichRun")
WHICH_RUN = "stage"

#print(WHICH_RUN)

bdc_env = "BI/Databricks/Marketing/Adobe/Stage"
if WHICH_RUN == "prod":
  bdc_env = "BI/Databricks/Marketing/Adobe/prod"
elif WHICH_RUN == "stage":
  bdc_env = "BI/Databricks/Marketing/Adobe/Stage"

hostname_adobe = dbutils.secrets.get(scope=bdc_env, key="hostname")
username_adobe = dbutils.secrets.get(scope=bdc_env, key="username")
password_adobe = dbutils.secrets.get(scope=bdc_env, key="password")
port_adobe = dbutils.secrets.get(scope=bdc_env, key="port")
gpg_key_adobe = dbutils.secrets.get(scope=bdc_env, key="gpg_key")
private_key_adobe = dbutils.secrets.get(scope=bdc_env, key="private_key")

# COMMAND ----------

sql_eso_vc_metrics = """ 
select derived_table3.customer_identifier as customer_identifier,
sum(derived_table3.pc_balance) as pc_balance,
sum(derived_table3.ps4_balance) as ps4_balance,
sum(derived_table3.xbox_balance) as xbox_balance,
date_format(max(derived_table3.last_transaction_time),'yyyy-MM-dd HH:mm:ss') as last_transaction_time,
sum(derived_table3.total_purchase_amount) as total_purchase_amount,
nvl(sum (derived_table3.total_crowns_ingested),0) as total_purchase_amount_crown_packs,
derived_table3.txn_date as day_of_crown_transaction,
derived_table3.daily_crowns_ingested_total,
derived_table3.daily_crowns_ingested_subs,
derived_table3.daily_crowns_ingested_crownpacks
FROM
(select derived_table2.user_account_id as customer_identifier,
case
    when derived_table2.platform = 'PC' then derived_table2.current_wallet else 0 end as pc_balance,
case
    when derived_table2.platform = 'Sony' then derived_table2.current_wallet else 0 end as ps4_balance,
case
    when derived_table2.platform = 'MSFT' then derived_table2.current_wallet else 0 end as xbox_balance,
    date_trunc('second',derived_table2.ctime_ts) as last_transaction_time,
derived_table2.ltd_purchased_amount_including_comp as total_purchase_amount,
crowns.total_crowns_ingested,
daily.txn_date,
daily.daily_crowns_ingested_total,
daily.daily_crowns_ingested_subs,
daily.daily_crowns_ingested_crownpacks
from
(
select derived_table1.user_account_id,derived_table1.platform,derived_table1.current_wallet,derived_table1.ltd_purchased_amount_including_comp,derived_table1.ingested_amount,
row_number() over (partition by derived_table1.user_account_id,derived_table1.platform order by
  CASE when (derived_table1.reason = 'Initial_Grant') then 1 else 2 end desc, derived_table1.ctime_ts desc) as row_number, derived_table1.ctime_ts
from
(
SELECT
  user_account_id
  , CASE
      WHEN realm_id IN (4000,4001) THEN 'PC'
      WHEN realm_id IN (4012,4014) THEN 'Sony'
      WHEN realm_id IN (4013,4015) THEN 'MSFT'
      ELSE null
    END AS platform
  , TO_DATE(FROM_UNIXTIME(ctime)) AS date
  ,  (purchased_amount - spent_amount) AS current_wallet
  , purchased_amount AS ltd_purchased_amount_including_comp
  , (purchased_amount - previous_purchased_amount) AS ingested_amount
  , ctime_ts
  ,vcdb.reason
FROM
  eso.virtual_currencydatabaseworker AS vcdb
WHERE
  realm_id IN (4000,4001,4012,4013,4014,4015)
   )derived_table1 
  )derived_table2 
  left join
  eso_reporting.eso_crowns_subs_transactions crowns
  on derived_table2.user_account_id = crowns.user_account_id
   left join
  (SELECT
  txn_date,
  user_account_id 
  , SUM(total_crowns_ingested) AS daily_crowns_ingested_total
  , SUM(CASE WHEN product_type = 'Subscription' THEN total_crowns_ingested ELSE 0 END) AS daily_crowns_ingested_subs
  , SUM(CASE WHEN product_type = 'Crown Pack' THEN total_crowns_ingested ELSE 0 END) AS daily_crowns_ingested_crownpacks
FROM
  eso_reporting.eso_crowns_subs_transactions
WHERE
  txn_date =  (DATE_FORMAT(current_date-3,"yyyy-MM-dd"))
  group by 1,2) daily
  on  derived_table2.user_account_id =daily.user_account_id 
   where derived_table2.row_number = 1
 ) derived_table3
 group by derived_table3.customer_identifier, derived_table3.txn_date, derived_table3.daily_crowns_ingested_total,
derived_table3.daily_crowns_ingested_subs,
derived_table3.daily_crowns_ingested_crownpacks"""

df_eso_vc_metrics = spark.sql(sql_eso_vc_metrics)
#display(df_eso_dlc)
    
df_eso_vc_metrics.write.format("csv").mode("overwrite").option("path","/mnt/bi-businessintelligence/adobe/integration/adobe_campaign_eso_vc_metrics").saveAsTable("adobe.adobe_campaign_eso_vc_metrics_int")

# COMMAND ----------

# %sql
# --refresh table adobe.adobe_campaign_eso_vc_metrics_int
# select count(*) from adobe.adobe_campaign_eso_vc_metrics_int  where day_of_crown_transaction is not null


# COMMAND ----------

# %sql
# select * from adobe.adobe_campaign_eso_dlc

# COMMAND ----------

df = spark.table("adobe.adobe_campaign_eso_vc_metrics_int")
df.coalesce(1).write.option("header","true").mode("overwrite").csv("s3a://bi-businessintelligence/adobe/integration/adobe_campaign_eso_vc_metrics")

# COMMAND ----------

# import os
# import glob
# files = glob.glob('/dbfs/mnt/bi-businessintelligence/adobe/integration/adobe_campaign_eso_vc_metrics/part*.csv')
# for file in files:
#     os.rename(file,'/dbfs/mnt/bi-businessintelligence/adobe/integration/adobe_campaign_eso_vc_metrics/crown_balance_000.csv' )

# COMMAND ----------

import os
import glob
import shutil

files = glob.glob('/dbfs/mnt/bi-businessintelligence/adobe/integration/adobe_campaign_eso_vc_metrics/part*.csv')
for file in files:
    shutil.copy(file,'/dbfs/mnt/bi-businessintelligence/adobe/integration/tosftp/vc_metrics')
    
files2 = glob.glob('/dbfs/mnt/bi-businessintelligence/adobe/integration/tosftp/vc_metrics/part*.csv')
for file in files2:
      os.rename(file,'/dbfs/mnt/bi-businessintelligence/adobe/integration/tosftp/vc_metrics/crown_balance_000.csv' )

# COMMAND ----------

file_path = '/dbfs/mnt/bi-businessintelligence/adobe/integration/tosftp/vc_metrics/'
file_name = 'crown_balance_000'
adobe_encrypt_export_sftp(file_path,file_name,hostname_adobe,username_adobe,password_adobe,port_adobe,gpg_key_adobe,private_key_adobe)
