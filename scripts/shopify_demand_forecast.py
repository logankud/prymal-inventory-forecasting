
import boto3
import base64
from botocore.exceptions import ClientError, ParamValidationError, WaiterError
import json
import requests
import pandas as pd
import numpy as np
import statistics
import datetime
from datetime import timedelta
import pytz
import os
import loguru
from loguru import logger
import re
import io

# -------------------------------------
# Variables
# -------------------------------------

REGION = 'us-east-1'



AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY=os.environ['AWS_ACCESS_SECRET']

start_time = datetime.datetime.now()
logger.info(f'Start time: {start_time}')

# -------------------------------------
# Functions
# -------------------------------------


# FUNCTION TO EXECUTE ATHENA QUERY AND RETURN RESULTS
# ----------

def run_athena_query(query:str, database: str, region:str):

        
    # Initialize Athena client
    athena_client = boto3.client('athena', 
                                 region_name=region,
                                 aws_access_key_id=AWS_ACCESS_KEY_ID,
                                 aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Execute the query
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': 's3://prymal-ops/athena_query_results/'  # Specify your S3 bucket for query results
            }
        )

        query_execution_id = response['QueryExecutionId']

        # Wait for the query to complete
        state = 'RUNNING'

        logger.info("Running query...")


        while (state in ['RUNNING', 'QUEUED']):
            response = athena_client.get_query_execution(QueryExecutionId = query_execution_id)
            if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in response['QueryExecution']['Status']:
                # Get currentstate
                state = response['QueryExecution']['Status']['State']

                if state == 'FAILED':
                    logger.error('Query Failed!')
                elif state == 'SUCCEEDED':
                    logger.info('Query Succeeded!')
            

        # OBTAIN DATA

        # --------------



        query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id,
                                                MaxResults= 1000)
        


        # Extract qury result column names into a list  

        cols = query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']
        col_names = [col['Name'] for col in cols]



        # Extract query result data rows
        data_rows = query_results['ResultSet']['Rows'][1:]



        # Convert data rows into a list of lists
        query_results_data = [[r['VarCharValue'] if 'VarCharValue' in r else np.NaN for r in row['Data']] for row in data_rows]



        # Paginate Results if necessary
        while 'NextToken' in query_results:
                query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id,
                                                NextToken=query_results['NextToken'],
                                                MaxResults= 1000)



                # Extract quuery result data rows
                data_rows = query_results['ResultSet']['Rows'][1:]


                # Convert data rows into a list of lists
                query_results_data.extend([[r['VarCharValue'] if 'VarCharValue' in r else np.NaN for r in row['Data']] for row in data_rows])



        results_df = pd.DataFrame(query_results_data, columns = col_names)
        
        return results_df


    except ParamValidationError as e:
        logger.error(f"Validation Error (potential SQL query issue): {e}")
        # Handle invalid parameters in the request, such as an invalid SQL query

    except WaiterError as e:
        logger.error(f"Waiter Error: {e}")
        # Handle errors related to waiting for query execution

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        if error_code == 'InvalidRequestException':
            logger.error(f"Invalid Request Exception: {error_message}")
            # Handle issues with the Athena request, such as invalid SQL syntax
            
        elif error_code == 'ResourceNotFoundException':
            logger.error(f"Resource Not Found Exception: {error_message}")
            # Handle cases where the database or query execution does not exist
            
        elif error_code == 'AccessDeniedException':
            logger.error(f"Access Denied Exception: {error_message}")
            # Handle cases where the IAM role does not have sufficient permissions
            
        else:
            logger.error(f"Athena Error: {error_code} - {error_message}")
            # Handle other Athena-related errors

    except Exception as e:
        logger.error(f"Other Exception: {str(e)}")
        # Handle any other unexpected exceptions



# Check S3 Path for Existing Data
# -----------

def check_path_for_objects(bucket: str, s3_prefix:str):

  logger.info(f'Checking for existing data in {bucket}/{s3_prefix}')

  # Create s3 client
  s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

  # List objects in s3_prefix
  result = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix )

  # Instantiate objects_exist
  objects_exist=False

  # Set objects_exist to true if objects are in prefix
  if 'Contents' in result:
      objects_exist=True

      logger.info('Data already exists!')

  return objects_exist

# Delete Existing Data from S3 Path
# -----------

def delete_s3_prefix_data(bucket:str, s3_prefix:str):


  logger.info(f'Deleting existing data from {bucket}/{s3_prefix}')

  # Create an S3 client
  s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

  # Use list_objects_v2 to list all objects within the specified prefix
  objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)

  # Extract the list of object keys
  keys_to_delete = [obj['Key'] for obj in objects_to_delete.get('Contents', [])]

  # Check if there are objects to delete
  if keys_to_delete:
      # Delete the objects using 'delete_objects'
      response = s3_client.delete_objects(
          Bucket=bucket,
          Delete={'Objects': [{'Key': key} for key in keys_to_delete]}
      )
      logger.info(f"Deleted {len(keys_to_delete)} objects")
  else:
      logger.info("No objects to delete")




def generate_daily_run_rate(daily_qty_sold_df: pd.DataFrame, inventory_df: pd.DataFrame, sku_value:str):

    logger.info(f'UPDATING FORECAST TABLE - {sku_value}')

    # Create date lists of recent dates to include to report on recent sales 
    # -----------

    daily_list = []

    for i in range(0,60):   # today through 60 days ago
        day = pd.to_datetime(pd.to_datetime('today') - timedelta(i)).strftime('%Y-%m-%d')
        daily_list.append(day)

    weekly_list = []

    for i in range(1,6):   # last week (last full week) through 6 weeks ago
        week = pd.to_datetime(pd.to_datetime('today') - timedelta(i * 7)).strftime('%Y-%W')
        weekly_list.append(week)

    # DAILY QTY SOLD
    # -----

    # Calculate daily dataframe
    daily_df = daily_qty_sold_df.loc[daily_qty_sold_df['sku']==sku_value].sort_values('order_date',ascending=False)
    sku_name = daily_df['sku_name'].unique().tolist()[0]

    # Subset inventory_df to get inventory details for the current SKU
    current_sku_inventory = inventory_df.loc[inventory_df['sku']==sku_value].copy()

    # Rename column to join with order data
    current_sku_inventory.rename(columns={'partition_date':'order_date'},inplace=True)

    # Subset / select specific columns
    current_sku_inventory = current_sku_inventory[['order_date', 'sku','inventory_on_hand']]

    # Calculate daily statistics for past 7, 14, 30 & 60 days (if no records, then set to 0)

    if len(daily_df.loc[daily_df['order_date'].isin(daily_list[:7]),'qty_sold']) > 0:


        date_df = pd.DataFrame(daily_list[:7],columns=['order_date'])
        # Join Series of dates to asses with order date & inventory on hand data
        daily_sales = date_df.merge(daily_df,on='order_date',how='left').fillna(0)
        daily_df_subset = daily_sales.merge(current_sku_inventory[['order_date','inventory_on_hand']],on=['order_date'],how='left').fillna(0)
        # Excude days when there was no inventory to sell 
        daily_df_subset = daily_df_subset.loc[(daily_df_subset['inventory_on_hand']>0)&(daily_df_subset['qty_sold']>0)]

        

        # Calculate inventory demand ration (% of available days where inventory was available that a sale occured) (to weight percentiles)
        days_ordered = len(daily_df.loc[daily_df['order_date'].isin(daily_list[:7]),'qty_sold'])
        days_available = len(daily_df_subset)
        # If inventory days_available = 0 and sales occured , then set inventory_demand_ratio to 1 to avoid divide by 0 error
        if days_available == 0:
            inventory_demand_ratio = 1
        else:
            inventory_demand_ratio = min(1,days_ordered / days_available)    # for cases where inventory was sold on more days than inventory was available, only allow a max of 1 for this ratio

        

        if len(daily_df_subset) > 0:
            last_7_median =  daily_df_subset['qty_sold'].median() * inventory_demand_ratio
            last_7_p25 = np.percentile(daily_df_subset['qty_sold'],25) * inventory_demand_ratio
            last_7_p75 = np.percentile(daily_df_subset['qty_sold'],75) * inventory_demand_ratio

        

        else:
            last_7_median = 0
            last_7_p25 = 0
            last_7_p75 = 0

    else:
        last_7_median = 0
        last_7_p25 = 0
        last_7_p75 = 0


    if len(daily_df.loc[daily_df['order_date'].isin(daily_list[:14]),'qty_sold']) > 0:


        date_df = pd.DataFrame(daily_list[:14],columns=['order_date'])
        # Join Series of dates to asses with order date & inventory on hand data
        daily_sales = date_df.merge(daily_df,on='order_date',how='left').fillna(0)
        daily_df_subset = daily_sales.merge(current_sku_inventory[['order_date','inventory_on_hand']],on=['order_date'],how='left').fillna(0)
        # Excude days when there was no inventory to sell 
        daily_df_subset = daily_df_subset.loc[(daily_df_subset['inventory_on_hand']>0)&(daily_df_subset['qty_sold']>0)]


        # Calculate inventory demand ration (% of available days where inventory was available that a sale occured) (to weight percentiles)
        days_ordered = len(daily_df.loc[daily_df['order_date'].isin(daily_list[:14]),'qty_sold'])
        days_available = len(daily_df_subset)
        # If inventory days_available = 0 and sales occured , then set inventory_demand_ratio to 1 to avoid divide by 0 error
        if days_available == 0:
            inventory_demand_ratio = 1
        else:
            inventory_demand_ratio = min(1,days_ordered / days_available)    # for cases where inventory was sold on more days than inventory was available, only allow a max of 1 for this ratio

        if len(daily_df_subset) > 0:
            last_14_median =  daily_df_subset['qty_sold'].median() * inventory_demand_ratio
            last_14_p25 = np.percentile(daily_df_subset['qty_sold'],25) * inventory_demand_ratio
            last_14_p75 = np.percentile(daily_df_subset['qty_sold'],75) * inventory_demand_ratio

        else:
            last_14_median = 0
            last_14_p25 = 0
            last_14_p75 = 0

    else:
        last_14_median = 0
        last_14_p25 = 0
        last_14_p75 = 0

    if len(daily_df.loc[daily_df['order_date'].isin(daily_list[:30]),'qty_sold']) > 0:

        date_df = pd.DataFrame(daily_list[:30],columns=['order_date'])
        # Join Series of dates to asses with order date & inventory on hand data
        daily_sales = date_df.merge(daily_df,on='order_date',how='left').fillna(0)
        daily_df_subset = daily_sales.merge(current_sku_inventory[['order_date','inventory_on_hand']],on=['order_date'],how='left').fillna(0)
        # Excude days when there was no inventory to sell 
        daily_df_subset = daily_df_subset.loc[(daily_df_subset['inventory_on_hand']>0)&(daily_df_subset['qty_sold']>0)]


        # Calculate inventory demand ration (% of available days where inventory was available that a sale occured) (to weight percentiles)
        days_ordered = len(daily_df.loc[daily_df['order_date'].isin(daily_list[:30]),'qty_sold'])
        days_available = len(daily_df_subset)
        # If inventory days_available = 0 and sales occured , then set inventory_demand_ratio to 1 to avoid divide by 0 error
        if days_available == 0:
            inventory_demand_ratio = 1
        else:
            inventory_demand_ratio = min(1,days_ordered / days_available)    # for cases where inventory was sold on more days than inventory was available, only allow a max of 1 for this ratio

        if len(daily_df_subset) > 0:
            last_30_median =  daily_df_subset['qty_sold'].median() * inventory_demand_ratio
            last_30_p25 = np.percentile(daily_df_subset['qty_sold'],25) * inventory_demand_ratio
            last_30_p75 = np.percentile(daily_df_subset['qty_sold'],75) * inventory_demand_ratio

        else:
            last_30_median = 0
            last_30_p25 = 0
            last_30_p75 = 0

    else:
        last_30_median = 0
        last_30_p25 = 0
        last_30_p75 = 0

    if len(daily_df.loc[daily_df['order_date'].isin(daily_list[:60]),'qty_sold']) > 0:

        date_df = pd.DataFrame(daily_list[:60],columns=['order_date'])
        # Join Series of dates to asses with order date & inventory on hand data
        daily_sales = date_df.merge(daily_df,on='order_date',how='left').fillna(0)
        daily_df_subset = daily_sales.merge(current_sku_inventory[['order_date','inventory_on_hand']],on=['order_date'],how='left').fillna(0)
        # Excude days when there was no inventory to sell 
        daily_df_subset = daily_df_subset.loc[(daily_df_subset['inventory_on_hand']>0)&(daily_df_subset['qty_sold']>0)]

        # Calculate inventory demand ration (% of available days where inventory was available that a sale occured) (to weight percentiles)
        days_ordered = len(daily_sales.loc[daily_sales['qty_sold']>0])
        days_available = len(daily_df_subset)
        # If inventory days_available = 0 and sales occured , then set inventory_demand_ratio to 1 to avoid divide by 0 error
        if days_available == 0:
            inventory_demand_ratio = 1
        else:
            inventory_demand_ratio = min(1,days_ordered / days_available)    # for cases where inventory was sold on more days than inventory was available, only allow a max of 1 for this ratio

            

        if len(daily_df_subset) > 0:

            last_60_median = daily_df_subset['qty_sold'].median()  * inventory_demand_ratio
            last_60_p25 = np.percentile(daily_df_subset['qty_sold'],25) * inventory_demand_ratio
            last_60_p75 =np.percentile(daily_df_subset['qty_sold'],75) * inventory_demand_ratio

        else:
            last_60_median = 0
            last_60_p25 = 0
            last_60_p75 = 0


    else:
        last_60_median = 0
        last_60_p25 = 0
        last_60_p75 = 0


    # Group by order week and sum qty sold
    weekly_df = daily_qty_sold_df.loc[daily_qty_sold_df['sku']==sku_value].groupby(['week','sku'],as_index=False).agg({'qty_sold':'sum',
                                                                                                                       'order_date':'nunique'}).sort_values('week',ascending=False)

    # Drop most recent week (in case not complete)
    weekly_df = weekly_df[1:].copy()

    # Add 'week' column to inventory on hand data
    current_sku_inventory['week'] = pd.to_datetime(current_sku_inventory['order_date']).dt.strftime('%Y-%W')
    # Calculate weekly inventory on hand df
    current_sku_inventory_weekly = current_sku_inventory.groupby(['week','sku'],as_index=False).agg({'inventory_on_hand':'median',
                                                                                                    'order_date':'nunique'})
    current_sku_inventory_weekly.columns = ['week','sku','inventory_on_hand','days_in_stock']


    # Calculate daily statistics for past 7, 14, 30 & 60 days
    if len(weekly_df.loc[weekly_df['week'].isin(weekly_list[:2]),'qty_sold']) > 0:

        date_df = pd.DataFrame(weekly_list[:2],columns=['week'])
        # Join Series of dates to asses with order date & inventory on hand data
        weekly_df_subset = date_df.merge(weekly_df,on='week',how='left').merge(current_sku_inventory_weekly,on=['week','sku'],how='left').fillna(0)
        # Excude weeks when there was no inventory to sell 
        weekly_df_subset = weekly_df_subset.loc[(weekly_df_subset['inventory_on_hand']>0)&(weekly_df_subset['qty_sold']>0)]

        # Calculate inventory demand ration (% of available days where inventory was available that a sale occured) (to weight percentiles)
        days_ordered = weekly_df.loc[weekly_df['week'].isin(weekly_list[:2]),'order_date'].sum()
        days_available = weekly_df_subset['days_in_stock'].sum()
        # If inventory days_available = 0 and sales occured , then set inventory_demand_ratio to 1 to avoid divide by 0 error
        if days_available == 0:
            inventory_demand_ratio = 1
        else:
            inventory_demand_ratio = min(1,days_ordered / days_available)    # for cases where inventory was sold on more days than inventory was available, only allow a max of 1 for this ratio



        if len(weekly_df_subset) > 0:
        
            last_2_weekly_median = weekly_df_subset['qty_sold'].median() * inventory_demand_ratio
            last_2_weekly_p25 =  np.percentile(weekly_df_subset['qty_sold'],25) * inventory_demand_ratio
            last_2_weekly_p75 = np.percentile(weekly_df_subset['qty_sold'],75) * inventory_demand_ratio

        else:
            last_2_weekly_median = 0
            last_2_weekly_p25 = 0
            last_2_weekly_p75 = 0
    else:
        last_2_weekly_median = 0
        last_2_weekly_p25 = 0
        last_2_weekly_p75 = 0

    if len(weekly_df.loc[weekly_df['week'].isin(weekly_list[:3]),'qty_sold']) > 0:

        date_df = pd.DataFrame(weekly_list[:3],columns=['week'])
        # Join Series of dates to asses with order date & inventory on hand data
        weekly_df_subset = date_df.merge(weekly_df,on='week',how='left').merge(current_sku_inventory_weekly,on=['week','sku'],how='left').fillna(0)
        # Excude weeks when there was no inventory to sell 
        weekly_df_subset = weekly_df_subset.loc[(weekly_df_subset['inventory_on_hand']>0)&(weekly_df_subset['qty_sold']>0)]

        # Calculate inventory demand ration (% of available days where inventory was available that a sale occured) (to weight percentiles)
        days_ordered = weekly_df.loc[weekly_df['week'].isin(weekly_list[:3]),'order_date'].sum()
        days_available = weekly_df_subset['days_in_stock'].sum()
        # If inventory days_available = 0 and sales occured , then set inventory_demand_ratio to 1 to avoid divide by 0 error
        if days_available == 0:
            inventory_demand_ratio = 1
        else:
            inventory_demand_ratio = min(1,days_ordered / days_available)    # for cases where inventory was sold on more days than inventory was available, only allow a max of 1 for this ratio


        if len(weekly_df_subset) > 0:
        
            last_3_weekly_median = weekly_df_subset['qty_sold'].median() * inventory_demand_ratio
            last_3_weekly_p25 = np.percentile(weekly_df_subset['qty_sold'],25) * inventory_demand_ratio
            last_3_weekly_p75 = np.percentile(weekly_df_subset['qty_sold'],75) * inventory_demand_ratio

        else:
            last_3_weekly_median = 0
            last_3_weekly_p25 = 0
            last_3_weekly_p75 = 0
    else:
        last_3_weekly_median = 0
        last_3_weekly_p25 = 0
        last_3_weekly_p75 = 0

    if len(weekly_df.loc[weekly_df['week'].isin(weekly_list[:4]),'qty_sold']) > 0:

        date_df = pd.DataFrame(weekly_list[:4],columns=['week'])
        # Join Series of dates to asses with order date & inventory on hand data
        weekly_df_subset = date_df.merge(weekly_df,on='week',how='left').merge(current_sku_inventory_weekly,on=['week','sku'],how='left').fillna(0)
        # Excude weeks when there was no inventory to sell 
        weekly_df_subset = weekly_df_subset.loc[(weekly_df_subset['inventory_on_hand']>0)&(weekly_df_subset['qty_sold']>0)]

        # Calculate inventory demand ration (% of available days where inventory was available that a sale occured) (to weight percentiles)
        days_ordered = weekly_df.loc[weekly_df['week'].isin(weekly_list[:4]),'order_date'].sum()
        days_available = weekly_df_subset['days_in_stock'].sum()
        # If inventory days_available = 0 and sales occured , then set inventory_demand_ratio to 1 to avoid divide by 0 error
        if days_available == 0:
            inventory_demand_ratio = 1
        else:
            inventory_demand_ratio = min(1,days_ordered / days_available)    # for cases where inventory was sold on more days than inventory was available, only allow a max of 1 for this ratio

    
        if len(weekly_df_subset) > 0:
            last_4_weekly_median = weekly_df_subset['qty_sold'].median() * inventory_demand_ratio
            last_4_weekly_p25 = np.percentile(weekly_df_subset['qty_sold'],25) * inventory_demand_ratio
            last_4_weekly_p75 = np.percentile(weekly_df_subset['qty_sold'],75) * inventory_demand_ratio
 
        else:
                last_4_weekly_median = 0
                last_4_weekly_p25 = 0
                last_4_weekly_p75 = 0
    else:
        last_4_weekly_median = 0
        last_4_weekly_p25 = 0
        last_4_weekly_p75 = 0

    if len(weekly_df.loc[weekly_df['week'].isin(weekly_list[:5]),'qty_sold']) > 0:

        date_df = pd.DataFrame(weekly_list[:5],columns=['week'])
        # Join Series of dates to asses with order date & inventory on hand data
        weekly_df_subset = date_df.merge(weekly_df,on='week',how='left').merge(current_sku_inventory_weekly,on=['week','sku'],how='left').fillna(0)
        # Excude weeks when there was no inventory to sell 
        weekly_df_subset = weekly_df_subset.loc[(weekly_df_subset['inventory_on_hand']>0)&(weekly_df_subset['qty_sold']>0)]

        # Calculate inventory demand ration (% of available days where inventory was available that a sale occured) (to weight percentiles)
        days_ordered = weekly_df.loc[weekly_df['week'].isin(weekly_list[:5]),'order_date'].sum()
        days_available = weekly_df_subset['days_in_stock'].sum()
        # If inventory days_available = 0 and sales occured , then set inventory_demand_ratio to 1 to avoid divide by 0 error
        if days_available == 0:
            inventory_demand_ratio = 1
        else:
            inventory_demand_ratio = min(1,days_ordered / days_available)    # for cases where inventory was sold on more days than inventory was available, only allow a max of 1 for this ratio

        if len(weekly_df_subset) > 0:
        
            last_5_weekly_median = weekly_df_subset['qty_sold'].median() * inventory_demand_ratio
            last_5_weekly_p25 = np.percentile(weekly_df_subset['qty_sold'],25) * inventory_demand_ratio
            last_5_weekly_p75 = np.percentile(weekly_df_subset['qty_sold'],75) * inventory_demand_ratio
        else:
            last_5_weekly_median = 0
            last_5_weekly_p25 = 0
            last_5_weekly_p75 = 0
    else:
        last_5_weekly_median = 0
        last_5_weekly_p25 = 0
        last_5_weekly_p75 = 0



    # Consolidate stats
    recent_stats_df = pd.DataFrame([[last_7_p25, last_7_median, last_7_p75, last_2_weekly_p25,last_2_weekly_median,last_2_weekly_p75],
                [last_14_p25, last_14_median, last_14_p75,last_3_weekly_p25,last_3_weekly_median,last_3_weekly_p75],
                [last_30_p25, last_30_median, last_30_p75,last_4_weekly_p25,last_4_weekly_median,last_4_weekly_p75],
                [last_60_p25, last_60_median, last_60_p75,last_5_weekly_p25,last_5_weekly_median,last_5_weekly_p75]],
                columns=['percentile_25','median','percentile_75','percentile_25_weekly','median_weekly','percentile_75_weekly'])


    # Calculate median of lower bound (median) and upper bound (75th percentile) , excluding days / weeks with 0 sold
    lower_bound_daily = recent_stats_df.loc[recent_stats_df['median']>0,'median'].median()
    upper_bound_daily = recent_stats_df.loc[recent_stats_df['percentile_75']>0,'percentile_75'].median()
    lower_bound_weekly = recent_stats_df.loc[recent_stats_df['median_weekly']>0,'median_weekly'].median()
    upper_bound_weekly = recent_stats_df.loc[recent_stats_df['percentile_75_weekly']>0,'percentile_75_weekly'].median()


    lower_bound_final = statistics.median([lower_bound_daily, lower_bound_weekly/7])
    upper_bound_final = statistics.median([upper_bound_daily, upper_bound_weekly/7])


    # Extrapolate out 30, 60, 90 days
    run_rate = ['forecast_daily',lower_bound_final,upper_bound_final]
    
    # Consolidate into dataframe
    df = pd.DataFrame([run_rate],
                columns=['forecast','lower_bound','upper_bound'])
    
    # Append product name & sku
    df['sku'] = sku_value
    df['sku_name'] = sku_name

    # Last week's actuals
    last_7_actual = daily_df.loc[pd.to_datetime(daily_df['order_date']) >= (pd.to_datetime('today') - timedelta(7))].groupby('sku',as_index=False)['qty_sold'].sum()
    last_7_actual.columns = ['sku','last_7_actual']
    # Fill na with 0 (if no units were sold in last 7 days)
    last_7_actual['last_7_actual'] =  last_7_actual['last_7_actual'].fillna(0)

    # Last 90 days actual 
    last_90_actual = daily_df.loc[pd.to_datetime(daily_df['order_date']) >= (pd.to_datetime('today') - timedelta(90))].groupby('sku',as_index=False)['qty_sold'].sum()
    last_90_actual.columns = ['sku','last_90_actual']
    # Fill na with 0 (if no units were sold in last 7 days)
    last_90_actual['last_90_actual'] =  last_90_actual['last_90_actual'].fillna(0)




    # Merge
    df = df.merge(last_7_actual, on='sku',how='left').merge(last_90_actual, on='sku',how='left').copy()


    # Set partition date to yesterday (data as of yesterday)
    df['partition_date'] = pd.to_datetime(pd.to_datetime('today') - timedelta(1)).strftime('%Y-%m-%d')


    return df[['sku','sku_name','forecast','lower_bound','upper_bound','last_7_actual','last_90_actual','partition_date']]



# ========================================================================
# Execute Code
# ========================================================================

DATABASE = 'prymal-analytics'
REGION = 'us-east-1'

#  ---------------------------------
#  SET DATA CUTOFF DATE (HOW FAR BACK TO LOOK IN ORDER DATA, INVENTORY ON HAND DATA)
#  ---------------------------------


lookback_cutoff_date = pd.to_datetime(pd.to_datetime('today') - timedelta(days=100)).strftime('%Y-%m-%d')    # Back to 6 weeks ago

#  ---------------------------------
#  QUERY ORDER DATA (JOINED WITH NORMALIZED SKU DATA)
#  ---------------------------------

# Construct query to pull data by product
# ----

QUERY = f"""SELECT a.partition_date
            , a.order_date
            , a.sku
            , a.sku_name
            , b.product_category
            , b.product_type
            , SUM(a.qty_sold) as qty_sold 
            FROM "prymal-analytics"."shopify_qty_sold_by_sku_daily" a
            LEFT JOIN (SELECT * 
                        FROM "prymal"."skus_shopify" 
                        WHERE load_date = (SELECT MAX(load_date) 
                                            FROM "prymal"."skus_shopify")
                                            ) b            
            ON a.sku = b.sku 
            
            WHERE a.partition_date >= DATE('{lookback_cutoff_date}')
            GROUP BY a.partition_date
            , a.order_date
            , a.sku
            , a.sku_name
            , b.product_category
            , b.product_type
            ORDER BY a.order_date ASC

            """

# Query datalake to get quantiy sold per sku for the last 120 days
# ----

result_df = run_athena_query(query=QUERY, database=DATABASE, region=REGION)
result_df.columns = ['partition_date','order_date','sku','sku_name','product_category','product_type','qty_sold']

logger.info(result_df.head(3))
logger.info(result_df.info())
logger.info(f"Count of NULL RECORDS: {len(result_df.loc[result_df['order_date'].isna()])}")
# Format datatypes & new columns
result_df['order_date'] = pd.to_datetime(result_df['order_date']).dt.strftime('%Y-%m-%d')
result_df['qty_sold'] = result_df['qty_sold'].astype(int)
result_df['week'] = pd.to_datetime(result_df['order_date']).dt.strftime('%Y-%W')

logger.info(f"MIN DATE: {result_df['order_date'].min()}")
logger.info(f"MAX DATE: {result_df['order_date'].max()}")


# Create dataframe of skus sold in the time range
skus_sold_df = result_df.loc[~result_df['sku_name'].isna(),['sku','sku_name']].drop_duplicates()


#  ---------------------------------
#  QUERY INVENTORY ON HAND DATA
#  ---------------------------------

# Construct query to pull latest (as of yesterday) inventory details 
# ----


QUERY =f"""

        SELECT partition_date
        , CAST(sku AS VARCHAR) as sku
        , MAX(total_fulfillable_quantity)
        FROM "prymal"."shipbob_inventory"  
        WHERE partition_date >= '{lookback_cutoff_date}'
        GROUP BY partition_date
        , CAST(sku AS VARCHAR)

                    """

# Query datalake to get current inventory details for skus sold in the last 120 days
# ----

inventory_df = run_athena_query(query=QUERY, database='prymal', region=REGION)

inventory_df.columns = ['partition_date','sku','inventory_on_hand']


logger.info('inventory_df')
logger.info(inventory_df.head(3))

# Format datatypes & new columns
inventory_df['inventory_on_hand'] = inventory_df['inventory_on_hand'].astype(int)


#  ---------------------------------
#  GENERATE DAILY RUN RATE FOR EACH SKU SOLD IN THE SELECTED TIME PERIOD ('lookback_cutoff_date')
#  ---------------------------------

# Blank df to store results
product_run_rate_df = pd.DataFrame()

# For each sku in products list, generate forecast using recent sales data
for sku in result_df['sku'].unique():

    # Generate daily run rates for the product
    df = generate_daily_run_rate(daily_qty_sold_df=result_df, inventory_df=inventory_df,sku_value=sku)

    # Extract product details to carry forward
    product_type = result_df.reset_index().loc[result_df['sku']==sku,'product_type'].values[0]
    product_category = result_df.reset_index().loc[result_df['sku']==sku,'product_category'].values[0]

    df['product_type'] = product_type
    df['product_category'] = product_category

    # Append to run rate dataframe
    product_run_rate_df = pd.concat([product_run_rate_df,df])

# Reset index
product_run_rate_df.reset_index(inplace=True,drop=True)

#  Replace nlls with 0
product_run_rate_df['last_7_actual'] =  product_run_rate_df['last_7_actual'].fillna(0)

# Merge run rate df with yesterday's partition of inventory df 
inventory_details_df = product_run_rate_df.merge(inventory_df,
                how='left',
                on=['partition_date', 'sku'])

# # Calculate days of stock on hand given the inventory on hand and daily run rate
# inventory_details_df['days_of_stock_onhand'] = round(inventory_details_df['inventory_on_hand'] / inventory_details_df['upper_bound'],0).astype(int)


logger.info(inventory_details_df['inventory_on_hand'].head())


# ------------------
# SUBSET BY PRODUCT TYPE
# ------------------


# Classic Flavor - 320 g 
# ---

# Subset df to only include Classic Flavor 320 g
classic_320g_df = inventory_details_df.loc[inventory_details_df['product_type']=='Classic Creamer - Large Bag'].copy()

# Limited Edition Flavor - 320 g 
# ---

lmtd_320g_df = inventory_details_df.loc[(inventory_details_df['product_type']=='Limited Edition Creamer - Large Bag')&(inventory_details_df['upper_bound']>0)].sort_values('inventory_on_hand',ascending=False)

# Coffee Beans (whole, ground, kcup)
# ---

coffee_df = inventory_details_df.loc[(inventory_details_df['product_category']=='Coffee Beans')&(inventory_details_df['upper_bound']>0)]

# Classic Flavor - Bulk Bag
# ---

bulk_bag_df = inventory_details_df.loc[(inventory_details_df['product_type']=='Classic Creamer - Bulk Bag')&(inventory_details_df['upper_bound']>0)].sort_values('inventory_on_hand',ascending=False).head(20)


# Sachets
# ---

sachet_df = inventory_details_df.loc[inventory_details_df['product_type'].str.contains('Sachet')].sort_values('inventory_on_hand',ascending=False).copy()


# Variety Pack - Kickstart 
# ---

vp_kickstart_df = inventory_details_df.loc[inventory_details_df['product_type']=='Variety Pack - Kickstart'].copy()


# ------------------
#  CONSOLIDATE INTO REPORT
# ------------------

# Concat df of all product types which will be included in the report
inventory_report_df = pd.concat([classic_320g_df,bulk_bag_df, sachet_df,coffee_df,vp_kickstart_df])

# reset index
inventory_report_df.reset_index(inplace=True,drop=True)

# Extend upper bound of forecast for 90, 120, 150 days to determine upcoming quarter inventory needs
inventory_report_df['forecast_90_days'] = round(inventory_report_df['upper_bound'] * 90,0)
inventory_report_df['forecast_120_days'] = round(inventory_report_df['upper_bound'] * 120,0)
inventory_report_df['forecast_150_days'] = round(inventory_report_df['upper_bound'] * 150,0)

# Subtract inventory on hand from forecasted qty to determine production needs
inventory_report_df['production_next_90_days'] = inventory_report_df['forecast_90_days'] - inventory_report_df['inventory_on_hand']
inventory_report_df['production_next_120_days'] = inventory_report_df['forecast_120_days'] - inventory_report_df['inventory_on_hand']
inventory_report_df['production_next_150_days'] = inventory_report_df['forecast_150_days'] - inventory_report_df['inventory_on_hand']

# Replace negative with 0 for any instances where there is sufficient inventory for the quarter
inventory_report_df.loc[inventory_report_df['production_next_90_days']<0,'production_next_90_days'] = 0
inventory_report_df.loc[inventory_report_df['production_next_120_days']<0,'production_next_120_days'] = 0
inventory_report_df.loc[inventory_report_df['production_next_150_days']<0,'production_next_150_days'] = 0

# Calculate forecasted stockout date
for r in range(len(inventory_report_df)):

    # Extract total inventory on hand
    inventory_on_hand = inventory_report_df.loc[r,'inventory_on_hand']

    # Extract value to be used as daily run rate
    daily_run_rate = inventory_report_df.loc[r,'upper_bound']

    # If daily run rate was 0 (forecasting zero demand) then manually set days_of_stock_on_hand to 0
    if daily_run_rate == 0:

        inventory_report_df.loc[r,'days_of_stock_on_hand'] = 0
    
    # Check if there is existing inventory on hand (since it is the denominator) - if so, calculate days of stock on hand 
    elif daily_run_rate > 0 and inventory_on_hand > 0:

        inventory_report_df.loc[r,'days_of_stock_on_hand'] = (inventory_on_hand / daily_run_rate).astype(int)

    # Else set days of stock on hand to 0
    else:

        inventory_report_df.loc[r,'days_of_stock_on_hand'] = 0


# Calculate forecasted stockout date
inventory_report_df['forecasted_stockout_date'] = inventory_report_df['days_of_stock_on_hand'].apply(lambda x: pd.to_datetime(pd.to_datetime('today') + timedelta(max(x,0))).strftime('%Y-%m-%d'))

logger.info(inventory_report_df.head())

inventory_report_df.to_csv(f'inventory_report_{pd.to_datetime("today").strftime("%Y%m%d")}.csv')

# CONFIGURE BOTO  =======================================


# Create s3 client
s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                          )

# Set bucket
BUCKET = os.environ['S3_PRYMAL_ANALYTICS']

# WRITE TO S3 =======================================

current_time = datetime.datetime.now()
logger.info(f'Current time: {current_time}')

# Log number of rows
logger.info(f'{len(inventory_df)} rows in current_inventory_df')


current_date = pd.to_datetime('today') - timedelta(hours=5)     # From UTC to EST

partition_y = pd.to_datetime(current_date).strftime('%Y') 
partition_m = pd.to_datetime(current_date).strftime('%m') 
partition_d = pd.to_datetime(current_date).strftime('%d') 


# Configure S3 Prefix
S3_PREFIX_PATH = f"reports/shopify_demand_forecasting/year={partition_y}/month={partition_m}/day={partition_d}/shopify_demand_forecasting_{partition_y}{partition_m}{partition_d}.csv"

# Check if data already exists for this partition
data_already_exists = check_path_for_objects(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)

# If data already exists, delete it .. 
if data_already_exists == True:
   
   # Delete data 
   delete_s3_prefix_data(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)


logger.info(f'Writing to {S3_PREFIX_PATH}')


with io.StringIO() as csv_buffer:
    inventory_report_df.to_csv(csv_buffer, index=False)

    response = s3_client.put_object(
        Bucket=BUCKET, 
        Key=S3_PREFIX_PATH, 
        Body=csv_buffer.getvalue()
    )

    status = response['ResponseMetadata']['HTTPStatusCode']

    if status == 200:
        logger.info(f"Successful S3 put_object response for PUT ({S3_PREFIX_PATH}). Status - {status}")
    else:
        logger.error(f"Unsuccessful S3 put_object response for PUT ({S3_PREFIX_PATH}. Status - {status}")

