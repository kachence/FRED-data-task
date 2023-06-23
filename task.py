import time
import boto3
import requests
import pandas as pd
import awswrangler as wr
import datetime as datetime
from datetime import date
from dateutil.relativedelta import relativedelta

# AWS & FRED credentials
AWS_KEY = 'AKIASFFJQSHS5JPBLKXZ'
AWS_SECRET = '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'
FRED_API_KEY = 'c16459cc1a5489bd385168bc48aa3fed'

# paths to S3 locations
S3_PATH = 's3://data-task/test/'
LAST_UPDATED_CPI_PATH = 's3://data-task/last_updated/cpi/'

# create a boto3 session with AWS credentials (unnecessary in Lambda with the right setup)
session = boto3.Session(
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name='eu-central-1'
)

def backfill_gaps(dates):
    # check for any gaps among the partitions
    df = pd.DataFrame(dates)
    df.columns = ['date']
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(by='date', inplace=True)
    df['date_diff'] = df.date.diff()
    gaps = df[df['date_diff'] > pd.Timedelta(days=1)]

    if not gaps.empty:
        pass
    print(gaps.head())

def store_data(df: pd.DataFrame(), path: str, data_type: str, partitions: list = [], mode: str = 'append'):
    # initialize error counter
    error_counter = 0

    # try to store data at most 3 times
    while error_counter < 3:
        try:
            # store data in S3
            wr.s3.to_parquet(df=df, path=path, partition_cols=partitions, mode=mode, dataset=True, boto3_session=session)
            break
        except Exception as e:
            # print error and wait for 30 seconds before trying again
            print(f'Error when storing {data_type} data in S3: {e.__str__}')
            time.sleep(30)
            error_counter += 1

def process_data(data: dict, column_name: str) -> pd.DataFrame():
    # get rid of unecessary columns
    df = pd.DataFrame(data).drop(columns=['realtime_start', 'realtime_end'])

    # convert the date column to the right format and rename the value column
    df['date'] = pd.to_datetime(df['date']).dt.date
    df.rename(columns={'value': column_name}, inplace=True)

    # ensure the column is in the right format (float)
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')

    if column_name != 'cpi_level':
        # fill NaN values with the value of the previous row
        df[column_name].fillna(method='ffill', inplace=True)

        # check for any gaps in the data
        df['date_diff'] = df.date.diff()
        gaps = df[df['date_diff'] > pd.Timedelta(days=1)]

        if not gaps.empty:
            # set the 'date' column as the DataFrame's index
            df.set_index('date', inplace=True)

            # convert the index to DatetimeIndex
            df.index = pd.DatetimeIndex(df.index)

            # forward-fill weekends and reset index
            df = df.resample('D').ffill().drop(columns='date_diff')
            df.reset_index(inplace=True)

    return df

def get_sp500_data(start_date: str = (date.today() - relativedelta(years=2)).strftime('%Y-%m-%d')) -> pd.DataFrame():
    # construct the URL for S&P 500 data and send the GET request
    start_date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') - relativedelta(days=7)).strftime('%Y-%m-%d')
    SP500_URL = f'https://api.stlouisfed.org/fred/series/observations?series_id=SP500&api_key={FRED_API_KEY}&observation_start={start_date}&sort_order=asc&file_type=json'
    response = requests.get(SP500_URL)

    # handle the response and obtain the data
    if response.status_code == 200:
        # extract the observations and process the data
        data_sp500 = response.json()['observations']
        df_sp500 = process_data(data_sp500, 'sp500_daily_price')

        # calculate the daily percentage change
        df_sp500['sp500_daily_percentage_change'] = df_sp500['sp500_daily_price'].pct_change(periods=1) * 100

        return df_sp500
    else:
        # Print error and return an empty dataframe
        print(f'Error code: {response.status_code}, error message: {response.text}')

        return pd.DataFrame()

def format_cpi_data(df: pd.DataFrame()):
    # calculate annual percentage change using the monthly percentage changes
    df['cpi_annual_percentage_change'] = (df['cpi_level'].pct_change(periods=1) * 100).rolling(window=12).sum()

    # set the 'date' column as an index of the dataframe
    df.set_index('date', inplace=True)

    # convert index to DatetimeIndex
    df.index = pd.DatetimeIndex(df.index)

    # get the most recent date in the dataframe
    most_recent_date = df.index.max()

    # obtain a dummy date by adding 1 month to the most recent date
    dummy_date = most_recent_date + pd.DateOffset(months=1)

    # add a dummy row with NaN values for all columns
    dummy_row = pd.DataFrame([[float('nan'), float('nan')]], columns=df.columns, index=[dummy_date])

    # append the new row to the DataFrame, which will allow us to resample the lastest month as well
    df = pd.concat([df, dummy_row])

    # resample the using daily frequency and forward-filling missing values
    df = df.resample('D').ffill()

    # reset the index and fix the name of the date column
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'date'}, inplace=True)

    # save last updated CPI date
    store_data(df=pd.DataFrame([{'date': most_recent_date}]), path=LAST_UPDATED_CPI_PATH, 
                data_type='last updated CPI', mode='overwrite')

    return df

def get_cpi_data(start_date: str = (date.today() - relativedelta(years=2)).strftime('%Y-%m-%d')) -> pd.DataFrame():
    # construct the URL for CPI data and send the GET request
    start_date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') - relativedelta(months=13)).strftime('%Y-%m-%d')
    CPI_URL = f'https://api.stlouisfed.org/fred/series/observations?series_id=CPIAUCSL&api_key={FRED_API_KEY}&observation_start={start_date}&sort_order=asc&file_type=json'
    response = requests.get(CPI_URL)

    # handle the response and obtain the data
    if response.status_code == 200:
        # extract the observations and format the data
        data_cpi = response.json()['observations']
        df_cpi = process_data(data_cpi, 'cpi_level')
        df_cpi = format_cpi_data(df=df_cpi)

        return df_cpi
        
    else:
        # Print error and return an empty dataframe
        print(f'Error code: {response.status_code}, error message: {response.text}')

        return pd.DataFrame()

def update_cpi_data(last_updated: pd.Timestamp, dates: list):
    # construct the URL for CPI data and send the GET request
    start_date = (date.today() - relativedelta(months=15)).strftime('%Y-%m-%d')
    CPI_URL = f'https://api.stlouisfed.org/fred/series/observations?series_id=CPIAUCSL&api_key={FRED_API_KEY}&observation_start={start_date}&sort_order=asc&file_type=json'
    response = requests.get(CPI_URL)

    # handle the response and obtain the data
    if response.status_code == 200:
        # extract the observations and get the latest date
        data_cpi = response.json()['observations']
        latest_date = pd.Timestamp(data_cpi[-1]['date'])

        last_updated = pd.Timestamp('2023-04-01') # REMOVE

        # update CPI entries only if API returned new data
        if latest_date > last_updated:
            # format the CPI data
            df_cpi = process_data(data_cpi, 'cpi_level')
            df_cpi = format_cpi_data(df=df_cpi)

            # get the dates for which the new CPI data is valid
            dates = [date for date in dates if datetime.datetime.strptime(date, '%Y-%m-%d') >= latest_date and
                      datetime.datetime.strptime(date, '%Y-%m-%d') < latest_date + relativedelta(months=1)]
            
            # obtain the paths to iterate over
            paths = [S3_PATH + 'date=' + date + '/' for date in dates]

            for path in paths:
                # get the old data and drop the columns with the NaN values
                df = wr.s3.read_parquet(path=path, boto3_session=session, dataset=True)
                df.drop(columns=['cpi_level', 'cpi_annual_percentage_change'], inplace=True)

                # get the data to write for that particular date
                df_cpi = df_cpi[df_cpi['date'] == df['date'].iloc[0]]

                # fix the format of the date columns and merge
                df['date'] = pd.to_datetime(df['date']).dt.date
                df_cpi['date'] = pd.to_datetime(df_cpi['date']).dt.date
                df = df.merge(df_cpi, on='date', how='left')

                # delete the old parquet and write the new one
                to_delete = wr.s3.list_objects(path=path, boto3_session=session)
                store_data(df=df, path=S3_PATH, data_type='CPI data', partitions=['date'])
                wr.s3.delete_objects(to_delete, boto3_session=session)
    else:
        # Print error and return an empty dataframe
        print(f'Error code: {response.status_code}, error message: {response.text}')

        return pd.DataFrame()

def main():
    try:
        # get most recent date from the partitions
        dates = wr.s3.list_directories(S3_PATH, boto3_session=session)
        dates = [date[date.rindex('date=') + 5:-1] for date in dates if 'date=' in date]
        dates.sort(reverse=True)
    except Exception as e:
        print(f'Error when reading the data: {e.__str__}')
        dates = []

    if not dates:
        # build the SP500 and CPI database from scratch
        df_sp500 = get_sp500_data()
        df_cpi = get_cpi_data()

        # merge the two dataframes and get only data from 2022 onwards
        df = df_sp500.merge(df_cpi, on='date', how='left')
        df = df[df['date'] >= pd.to_datetime('2022-01-01')]

        # convert to date format so partitions look like YY-MM-DD
        df['date'] = pd.to_datetime(df['date']).dt.date

        # store S&P500 and CPI data
        store_data(df=df, path=S3_PATH, data_type='SP500 and CPI', partitions=['date'])
    else:
        # backfill gaps only on Mondays
        backfill_gaps(dates=dates)
        # add new S&P500 data
        df_sp500 = get_sp500_data(dates[0])
        df_sp500 = df_sp500[df_sp500['date'] > dates[0]]

        # convert to date format so partitions look like YY-MM-DD
        df_sp500['date'] = pd.to_datetime(df_sp500['date']).dt.date

        if not df_sp500.empty:
            # CPI data not available for today
            df_sp500['cpi_level'] = float('nan')
            df_sp500['cpi_annual_percentage_change'] = float('nan')

            # store S&P500 data
            store_data(df=df_sp500, path=S3_PATH, data_type='SP500', partitions=['date'])

        # get last updated cpi date
        last_updated = wr.s3.read_parquet(path=LAST_UPDATED_CPI_PATH, boto3_session=session)['date'].iloc[0]

        # check for new CPI data and update parquets if there is any
        update_cpi_data(last_updated=last_updated, dates=dates)

        # backfill gaps only on Mondays
        today = date.today()
        if today.weekday == 0:
            backfill_gaps()

        # trigger Glue crawl (recrawl all if cpi data was updated)

if __name__ == "__main__":
    main()
