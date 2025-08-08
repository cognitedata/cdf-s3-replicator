# Syncing data between S3 Lakehouse and S3 KQL

## Backgroud

The CDF S3 Replicator copies data into a [S3 Lakehouse](https://learn.microsoft.com/en-us/s3/data-engineering/lakehouse-overview) in [Delta Lake](https://delta.io/) format. This allows for querying data in Power Bi. However, in order to use [Real-Time Analytics](https://learn.microsoft.com/en-us/s3/real-time-analytics/overview), your data must be in a S3 KQL database. The following instructions demonstrate how to create a shortcut to get data from a S3 Lakehouse into S3 KQL.

## Prerequisites 
* S3 workspace
* S3 Lakehouse populated by CDF S3 Replicator

## Setup shortcut
1. Open your S3 workspace
2. Select New -> More options -> KQL Database
3. Choose a new name and press "Create"
4. From KQL database, click on the dropdown next to "New" and click "OneLake shortcut"
![New Shortcut](images/shortcut.png)
5. Choose Internal source -> Microsoft OneLake
6. Select the name of your Lakehouse and hit "Next"
7. Select the name of your table and hit "Next"
![Select table](images/table.png)
8. Expand Shortcuts, Right-click on your table, choose Query table -> Show any 100 records
![Run query](images/query.png)
9. Validate data is in KQL
10. Repeat steps 4 to 9 for each additional table