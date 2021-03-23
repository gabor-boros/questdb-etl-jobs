In the world of big data, software developers and data analysts often have to write scripts or complex software collections to process data before sending it to a data store for further analysis. This process is commonly called ETL, which stands for Extract, Transform and Load.

## What could we use ETL jobs for?

Let's consider the following example: a medium-sized webshop with a few thousand orders per day exports order information hourly. After a while, we would like to visualize purchase trends, and we might want to share the results between departments or even publicly. Since the exported data contains personally identifiable information (PII), we should anonymize it before using or exposing it to the public.

For the example above, we can use an ETL job to extract the incoming data, remove any PII and load the transformed data into a database used as the data visualization backend later.

## Prerequisites

During this tutorial, we will use Python to write the cloud functions, so basic python knowledge is essential. Aside from these skills, you will need the following resources:

- A [Google Cloud Platform](https://console.cloud.google.com/getting-started) (GCP) account and a GCP Project.
- Enable the [Cloud Build API](https://console.cloud.google.com/marketplace/product/google/cloudbuild.googleapis.com) - when enabling APIs **ensure that the correct GCP project is selected**.

## Creating an ETL job

As an intermediate datastore where the webshop exports the data, we will use Google Storage and use Google Cloud Functions to transform it before loading it into QuestDB.
![Graph showing tutorial workflow](./post/img/workflow-1.png)
We won't be building a webshop or a data exporter for an existing webshop, but we will use a script to simulate the export to Google Storage.

In the following sections, we will set up the necessary components on GCP. Ensure the required APIs mentioned in the prerequisites are enabled, and that you have selected the GCP project in which you would like to create the tutorial resources.

### Create a Compute Engine instance for QuestDB

First things first, we start with installing QuestDB on a virtual machine. To get started, navigate to the [Compute Engine console](https://console.cloud.google.com/compute/instances). Visiting this page for the first time will take a few moments to initialize. After the loading indicator has gone, start a new virtual machine:

1. Click on "create" and give the instance the name `questdb-vm`
2. Select a region close to you
3. Select the first generation "N1" series
4. Choose the `f1-micro` machine type - in a production environment you would choose a more performant instance, but for tutorial purposes, this is enough
5. In the "Firewall" section, click on "Management, security, disks, networking, sole tenancy"
6. In the newly panel, select "Networking" and add `questdb` as a "Network tag"
7. Leave all other settings with their defaults, and click on "create"

Make sure you note the "External IP" of the instance as we will need that later.

![GoogleCloud Platform showing active compute engine instances](./post/img/Screenshot-2021-03-20-at-17.18.41.png)

After a short time, the new instance will be up and running. As soon as the instance is provisioned, we can initiate a remote session to install QuestDB by clicking **ssh** in the VM panel.

### Install QuestDB on Compute Engine

Installing QuestDB on a Linux VM like Compute Engine is easy. In the terminal shell opened by clicking "ssh" do the following:

```bash
# download the latest binary and uncompress the contents
curl -L -o questdb.tar.gz https://github.com/questdb/questdb/releases/download/5.0.6.1/questdb-5.0.6.1-rt-linux-amd64.tar.gz
sudo mkdir /usr/local/bin/questdb && sudo tar -xvf questdb.tar.gz -C /usr/local/bin/questdb

# Run QuestDB
sudo /usr/local/bin/questdb/questdb-5.0.6.1-rt-linux-amd64/bin/questdb.sh start
```

If QuestDB has started successfully, you will see the following:

```txt
    / _ \ _   _  ___  ___| |_|  _ \| __ )
   | | | | | | |/ _ \/ __| __| | | |  _ \
   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
    \__\_\\__,_|\___||___/\__|____/|____/
                        www.questdb.io
JAVA: /usr/local/bin/questdb/questdb-5.0.6.1-rt-linux-amd64/bin/java
Created QuestDB ROOT directory: /root/.questdb
QuestDB server 5.0.6.1
Copyright (C) 2014-2021, all rights reserved.
```

You can additionally execute the following command to return the process ID (PID) of QuestDB:

```bash
sudo /usr/local/bin/questdb/questdb-5.0.6.1-rt-linux-amd64/bin/questdb.sh status
```

### Allow networking on the instance

If we try to open the web console by opening the `http://<EXTERNAL_IP>:9000` (where `<EXTERNAL_IP>` is the external IP of your virtual machine) it won't load and we will face a timeout. The reason behind this is that the firewall is not opened for port `9000` yet.

To allow port `9000` used by QuestDB, we must allow the port by adding a new firewall rule on the [firewall console](https://console.cloud.google.com/networking/firewalls/list):

1. Click on "create firewall rule" at the top of the page
2. Give the rule the name "QuestDBPorts"
3. In the "Target tags" field, write the same tag used for instance creation (`questdb`)
4. For the "Source IP ranges" field, set `0.0.0.0/0`
5. In the "Protocols and ports" section, select **tcp** and set port to `9000,8812`
6. Click on "create"

Some seconds later, the rule will be applied on every instance with the matching `questdb` tag and port `9000` will be open. You may ask what port `8812` is for; this port will be used by the Cloud Function later to connect to the database.

If you try to open the interactive console again, you should see the [QuestDB Web Console](https://questdb.io/docs/reference/web-console/) and start writing queries.

![QuestDB Web Console running on Google Cloud Platform](./post/img/Screenshot-2021-03-20-at-18.05.55.png)

As our first query, create the table in which the Cloud Function will write the anonymized data. To create the table run the following SQL statement:

```sql
CREATE TABLE
    purchases(buyer STRING, item_id INT, quantity INT, price INT, purchase_date TIMESTAMP)
    timestamp(purchase_date);
```

The query above uses `timestamp(purchase_date)` to set a designated timestamp on the table so we can easily perform time series analysis in QuestDB.
For more information on designated timestamps, see the official [QuestDB documentation for timestamp](https://questdb.io/docs/concept/designated-timestamp/).

### Create a Storage bucket

Now, we create the bucket where we will store the simulated webshop data. Storage buckets are in a single global namespace in GCP, which means that the bucket's name must be unique across all GCP customers. You can read more about Storage and buckets on Google's [documentation](https://cloud.google.com/storage/docs) site.

To create a new bucket:

1. Navigate to the [cloud storage console](https://console.cloud.google.com/storage)
2. Select your project if not selected yet
3. Click on "create bucket" and choose a unique name
4. Select the same region as the instance above
5. Leave other settings on default and click on "continue" to create the bucket

If you successfully created the bucket, it should show up in the storage browser as you can see below.

![A new bucket storage resource on Google Cloud Platform](./post/img/Screenshot-2021-03-20-at-10.10.08.png)

At this point, we don't set any permissions, ACLs, or visibility settings on the bucket, but we will come back to that later.

### Create a Cloud Function

We have the bucket to upload the data, but we have nothing to process the data yet, and for this, we will use Cloud Functions to remove the PII.

Cloud Functions are functions as a service (FaaS) solution within GCP, similar to AWS Lambda. The functions are triggered by an event that can come from various sources. Our scenario Cloud Functions are convenient since we don't need to pay for a server to run all day, which is mostly idle; the function will be executed when the trigger event is fired, and we only pay for the execution time the number of function calls.

To create a Cloud Function:

1. Navigate to [cloud functions console](https://console.cloud.google.com/functions/list)
2. Click on "create function" and give it the name `remove-pii`
3. Select the region we are using for other resources
4. For "Trigger" select "Cloud Storage" from the dropdown list
5. Set the event type to "Finalise/Create"
6. Choose the bucket created above and click "variables, networking, and advanced settings"
7. Select "environment variables" on the tabbed panel
8. Click on "add variable" right below the "Runtime environment variables" section
9. Add a new variable called `DATABASE_URL` with the value `postgresql://admin:quest@<EXTERNAL_IP>:8812/qdb`, where `<EXTERNAL_IP>` is the external IP of your virtual machine
10. Click "save" then "next"

![Creating a new Cloud Function on Google Cloud Platform for an ETL job](./post/img/Screenshot-2021-03-20-at-12.06.29.png)

The next step is to select the runtime our function will use and provide the code. On this page, we can choose between numerous runtimes, including multiple versions of Python, NodeJS, Go, Ruby, and even Java.

Since this tutorial uses Python, select **Python 3.8** as it is the latest non-beta version at the time of writing. Leave the rest of the settings as default, and write the function in the next section. Click "deploy" at the bottom of the page. Some seconds later, you will see that the deployment of the function is in progress.

![Deploying a Python3.8 Cloud Function on Google Cloud Platform](./post/img/Screenshot-2021-03-20-at-12.35.16.png)

The deployment may take a while, so we can move on to the next section of the tutorial.

## Generating and processing data

Before moving on, here's a quick recap on what we did so far:

- Set up a new Google Storage bucket
- Created the Cloud Function which will process the data later on
- Connected the bucket with the function to trigger on a new object is created on the bucket

Now for the fun part of the tutorial: writing the data processing script and loading the data in the database. Let's write the function to remove PII, but first, talk a bit about the data's structure.

### Inspect the data structure

ETL jobs, by their nature, heavily depend on the structure of incoming data. A job may process multiple data sources, and data structure can vary per source.
The data structure we will use is simple. We have a CSV file with the following information: 

* purchase date
* email address
* purchased item's ID
* quantity
* the price per item

As you see, there is no currency column since we will assume every price is in one currency.

To generate random data, you can use the [pre-made script](https://github.com/gabor-boros/questdb-etl-jobs/blob/e47b5f3191c8648f486cea207b317c92899c3bd1/data_generator.py) written for this tutorial.

### Create the data transformer function

By now, we have everything to write the data transformer function, connect the dots and try out the PII removal.

We will work in the "inline editor" of the cloud function, so as a first step, open the edit the cloud function created above by navigating to the [cloud functions console](https://console.cloud.google.com/functions/list) and clicking on the function's name. That will open the details of the function. At the top, click on "edit", then at the bottom, click on "next" to open the editor.

Let's start with the requirements. On the left-hand side, click on the `requirements.txt` and paste the following:

```txt
google-cloud-storage==1.36.2
psycopg2==2.8.6
sqlalchemy==1.4.2
```

Here we add the required packages to connect to Google Storage and QuestDB. Next, click on `main.py`, remove its whole content and start adding the following:

```python
import csv
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import List

from google.cloud import storage
from sqlalchemy.sql import text
from sqlalchemy.engine import Connection, create_engine

logger = logging.getLogger(__name__)

# Create a database engine
engine = create_engine(os.getenv("DATABASE_URL"))

# ...
```

As you may expect, we start with the imports, but we added two extra lines: one for the logger and one for configuring the database engine. We will need the logger to log warnings and exceptions during the execution, while we will use the engine later to insert anonymized data into the database.

To make our job easier, we are going to add a data class, called `Record`. This data class will be used to store the parsed and anonymized CSV data for a line of the uploaded file.

```python
# ...

@dataclass
class Record:
    buyer: str
    item_id: int
    quantity: int
    price: int
    purchase_date: datetime
    
# ...
```

As we discussed, ETL jobs are validating the data that they receive as input. In our case, we will trigger the function if an object is created on the storage. This means any object, like a CSV, PDF, TXT, PNG file, or even a directory, is created, though we only want to execute CSV files' transformation. To validate the incoming data, we write two simple validator functions:

```python
# ...

def is_event_valid(event: dict) -> bool:
    """
    Validate that the event has all the necessary attributes required for the
    execution.
    """

    attributes = event.keys()
    required_parameters = ["bucket", "contentType", "name", "size"]

    return all(parameter in attributes for parameter in required_parameters)


def is_object_valid(event: dict) -> bool:
    """
    Validate that the finalized/created object is a CSV file and its size is
    greater than zero.
    """

    has_content = int(event["size"]) > 0
    is_csv = event["contentType"] == "text/csv"

    return has_content and is_csv

# ...
```

The first function will validate that event has all the necessary parameters, while the second function checks that the object created and triggered the event is a CSV and has any content.
The next function we create is used to get an object from the storage which, in our case, the file triggered the event:

```python
# ...

def get_content(bucket: storage.Bucket, file_path: str) -> str:
    """
    Get the blob from the bucket and return its content as a string.
    """

    blob = bucket.get_blob(file_path)
    return blob.download_as_string().decode("utf-8")

# ...
```

Anonymizing the data, in this scenario, is relatively easy, though we need to ensure we can build statistics and visualizations later based on this data, so the anonymized parts should be consistent for a user. To achieve this, we will hash the buyer's email address, so nobody may track it back to the person owning the email, but we can use it for visualization:

```python
# ...

def anonymize_pii(row: List[str]) -> Record:
    """
    Unpack and anonymize data.
    """

    email, item_id, quantity, price, purchase_date = row

    # Anonymize email address
    hashed_email = hashlib.sha1(email.encode()).hexdigest()

    return Record(
        buyer=hashed_email,
        item_id=int(item_id),
        quantity=int(quantity),
        price=int(price),
        purchase_date=purchase_date,
    )

# ...
```

So far, we have functions to validate the data, get the file's content which triggered the Cloud Function, and anonymize the data. The next thing we need to be able to do is to load the data into our database. Up to this point, every function we have wrote was simple, and this one is no exception:

```python
# ...

def write_to_db(conn: Connection, record: Record):
    """
    Write the records into the database.
    """

    query = """
    INSERT INTO purchases(buyer, item_id, quantity, price, purchase_date)
    VALUES(:buyer, :item_id, :quantity, :price, to_timestamp(:purchase_date, 'yyyy-MM-ddTHH:mm:ss'));
    """

    try:
        conn.execute(text(query), **record.__dict__)
    except Exception as exc:
        # If an error occures, log the exception and continue
        logger.exception("cannot write record", exc_info=exc)

# ...
```

As you see, writing to the database is easy. We get the connection and the record we need to write into the database, prepare the query and execute it. In case of an exception, we don't want to block the whole processing, so we catch the exception, log it and let the script go on. If an exception occurred, we can check it later and fix the script or load the data manually.

The last bit is the glue code, which brings together these functions. Let's have a look at that:

```python
# ...

def entrypoint(event: dict, context):
    """
    Triggered by a creation on a Cloud Storage bucket.
    """

    # Check if the event has all the necessary parameters. In case any of the
    # required parameters are missing, return early not to waste execution time.
    if not is_event_valid(event):
        logger.error("invalid event: %s", json.dumps(event))
        return

    file_path = event["name"]

    # Check if the created object is valid or not. In case the object is invalid
    # return early not to waste execution time.
    if not is_object_valid(event):
        logger.warning("invalid object: %s", file_path)
        return

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(event["bucket"])

    data = get_content(bucket, file_path)
    reader = csv.reader(data.splitlines())

    # Anonymize PII and filter out invalid records
    records: List[Record] = filter(lambda r: r, [anonymize_pii(row) for row in reader])

    # Write the anonymized data to database
    with engine.connect() as conn:
        for record in records:
            write_to_db(conn, record)
``` 

In the example above, we call the two validators to ensure it worth processing the data, and we get the file path from the event. After that we initialize the client used to connect to Google Storage, then we get the object's content, parse the CSV file, and anonymize the content of it.

Last but not least, we connect to the database - defined by the DATABASE_URL configured for the engine and write all records to the database one by one.

As you see, the entrypoint of the function has been changed as well. In the text box called "Entrypoint" set the entrypoint as a function name to call. The entrypoint is the function that will be called by Cloud Functions when an event is triggered.

![Editing a Cloud Function on Google Cloud Platform](./post/img/Screenshot-2021-03-20-at-19.01.35.png)

## Connecting the services

We are close to finishing this tutorial, so it's time to test our Cloud Function. 

To test the Cloud Function:

1. Download the [pre-made script](https://github.com/gabor-boros/questdb-etl-jobs/blob/e47b5f3191c8648f486cea207b317c92899c3bd1/data_generator.py) and run it to generate random data.
2. Navigate to the bucket you created
3. Above the list of objects in the bucket (which should be empty) click on "upload files"
4. Select and upload the random generated data *1
5. After the file is uploaded, go to [Cloud Functions console](https://console.cloud.google.com/functions/list)
6. Click on the actions button and select "view logs"
7. Calidate that the script did not encounter any issues
8. Navigate to `http://<EXTERNAL_IP>:9000`, where `<EXTERNAL_IP>` is the external IP of your virtual machine

We can now execute the following SQL query:

```sql
SELECT * FROM purchases ORDER BY purchase_date;
```

As you can see, the data is loaded and we have no PII there. By creating a simple chart, we can even observe trends in the generated data, how our imaginary buyers purchased items on the webshop.

![Visualizing SQL query results in the QuestDB Web Console](./post/img/Screenshot-2021-03-20-at-19.50.45.png)

*1 QuestDB at the time of writing does not support "out of order" writes. This means you need to upload data with delay to let the previous function finish processing the data. Also, the uploaded purchase data must be in time order and increasing across the uploads. Example: We are uploading `data1.csv` and `data2.csv` the last generated purchase data in `data1.csv` is `2021-03-21T11:59:49`, therefor `data2.csv`'s first purchase order must be greater than or equal to `2021-03-21T11:59:49`.

## Summary

We've installed QuestDB on Google Cloud Platform, set up a Google Storage bucket to store the simulated purchase data exports, built an ETL job that anonymized our buyers' data, and loaded it into a time series database, QuestDB. Data analysts could write more jobs as Cloud Functions in multiple languages and set up multiple sources. Furthermore, this data could be loaded into a business intelligence (BI) dashboard like Power BI to have a more comprehensive overview of the data as it does not contains PII anymore.

Thank you for your attention!

_The source code is available at [https://github.com/gabor-boros/questdb-etl-jobs](https://github.com/gabor-boros/questdb-etl-jobs)._
