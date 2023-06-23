# Overview

This solution to the ETL pipeline task for storing data fetched from the FRED API approaches the task at hand by utilizing various AWS services, all serving a different purpose for creating a robust, efficient, and reliable ETL pipeline. Namely, the services that this solution uses are: S3, Lambda, Glue, Athena, CloudWatch, and EventBridge. Additionally, all of the code is written in Python, data formatting is done with Pandas, and to connect to the Amazon services the AWS SDK for pandas (awswrangler) was used.

The following sections explain the design choices made when approaching this task, including the architecture of the solution, how the code should be ran, and the provided interface to access the data.

## Dataset Creation 

As defined in the task, the final output is a dataset partitioned by a **date** column and four additional columns - S&P500 index daily price (**sp500_daily_price**), S&P500 daily percentage change (**sp500_daily_percentage_change**), CPI index level (**cpi_level**), and CPI index annual percentage change (**cpi_annual_percentage_change**). Only the CPI columns may contain null values for when data is not yet available. The correct data types are enforced on all columns to ensure that the **Athena** table doesn't break. You can have a look of a preview of the table in the below image:

![Preview of data in Athena query editor](/images/athena.png)

The dataset is updated on a daily basis on weekdays at 22:15 (UTC) to ensure we fetch data after S&P500 market has closed and after CPI data is released. Any missing data (whether it is weekends or holidays) is forward filled to avoid gaps in the data. We are using **EventBridge** to schedule the **Lambda** that takes care of all the necessary processes. You can find the cron expression used to schedule the running of the **Lambda** script in the following image: 

![EventBridge cron expression](/images/eventbridge.png)

The motivation behind using **Lambda** to run the script is that the execution time of the script does not go over a minute or so even in some exceptional cases (e.g., building the whole table from scratch). Lambdas are a cheap and convenient way to run scripts that execute for less than 15 minutes. In our case, it is sufficient to have **512 MB** of RAM memory and **1024 MB** of Ephemeral storage. Additionally, setting the runtime (**Python 3.10**) and layers (**AWSSDKPandas-Python310**) is quite straightforward as the ones available from the AWS interface suffice.

Finally, it is necessary to run a Glue crawler so that

## Storage & Accessing Data

**S3** was used to store the data for this task. The motivation behind this choice is that **S3** is really cheap and it allows for a fairly quick access to the data (when the data has been crawled by a **Glue** crawler to create an **Athena** table). One of the main drawbacks of using **S3** is that it is somewhat troublesome when having to edit the existing parquet files, but in this case this is not much of a problem. The reason for using parquet files is that they are pretty universal when it comes to storing data of most types and they are quite efficient in terms of disk space. Partitions (by date) are also used in this solution to optimize SQL queries when filtering data on specific dates.

Once a **Glue** crawler has completed crawling and updating the **Athena** table, anyone with access can quickly read the data from the database. This can be done both from the **Athena** query editor interface in AWS, and also in Python by using the **awswrangler.athena** API for executing queries.

## Recovery Mechanism

To ensure that there is no missing data, the solution incorporates several resampling and backfilling solutions. Whenever data is fetched it gets resampled to fill any missing entires (holidays and weekends). Additionally, there is a check that find any potential gaps in the data (by iterating over the date partitions) and backfills data accordingly.

The script also ensures that rate limit errors or other issues (such as network failures) do not break its functionality. Whenever such an event occurs, the exception is handled and the script waits for one minute before retrying to fetch the data again.

## Monitoring

When it comes to monitoring, the solution makes use of the **CloudWatch** service provided by AWS, which is a very simple and yet an efficient way of keeping track of the execution of all processes. The biggest margin for error lies in the execution of the **Lambda** script and the **Glue** crawler, and **CloudWatch** keeps track of them both, providing the necessary logs with their respective timestamps.

A more sophisticated approach to monitoring these processes would be to implement **Airflow**, which will allow to break down the processes and keep track of their individual execution, significantly improving error readability.
