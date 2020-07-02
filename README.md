# Google Analytics ETL

A Python base class to extract and continuously sync data from Google Analytics, and write it to a SQL database.

Uses [Analytics Reporting API v4](https://developers.google.com/analytics/devguides/reporting/core/v4) and it works similarly to Goole Analytics Custom Reports where you define a list of dimensions to be displayed in a flat table. But doesn't have the limitations of GA's UI. In addition, data can be custom transformed and will be written to a SQL database.

## Installation

### Requirements

You should prefer Python 3 packages from your operating system.

So under Fedora Linux:

```shell
dnf install python3-sqlalchemy python3-pandas python3-mysql
```
Under Red Hat Enterprise Linux 8:

```shell
dnf install python3-sqlalchemy mariadb-connector-c-devel mariadb-connector-c gcc
pip3 install mysqlclient --user
```
This MariaDB connector is the one that works with SQLAlchemy (used by the module). Other connectors as PyMySQL failed our tests.

If you chose to installing `mysqlclient` with `pip`, you'll require compilers and MariaDB development framework pre-installed in the system, as shown in the Red Hat Enterprise Linux section. But avoid doing it like that and use your OS's pre-compiled packages as shown above.

With SQLAlchemy, you can use other backend database systems such as DB2, Oracle, PostgreSQL, SQLite. Just make sure you have the correct SQLAlchemy driver installed.

### Install the module

```shell
pip3 install GoogleAnalyticsETL --user
```

All unsatisfied dependencies (such as Pandas on RHEL) will be installed along.

Or, to upgrade:

```shell
pip3 install -U GoogleAnalyticsETL --no-deps --user
```
Remove the `--no-deps` if you wish to upgrade also dependent modules as pandas and numpy.

## Usage

Extracting data from Google Analytics is not easy due to its variety, complexity and amount of data, specially if you have a busy site with custom dimensions (GA Admin ➔ Property ➔ Custom Definitions ➔ Custom Dimensions).

So use the example file `examples/GABradescoSegurosToDB.py`. Copy it to a new place and edit it to create your own concrete classes with desired dimensions.

Then copy `examples/etl.py` and edit it to call your custom classes. This will be the executable script that you'll run every time.

### 1. Get credentials and API keys from Google

Follow [instruction on GA Reporting API v4](https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py#1_enable_the_api) to enable your Analytics instance to be accessed by this classes and to get credentials.

Also collect the IDs of the GA View that will be accessed. Get the account number, GA property and View ID from [Account Explorer](https://ga-dev-tools.appspot.com/account-explorer/).

### 2. Prepare a list of comprehensive default dimensions

Use [Dimensions & Metrics Explorer](https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/) to find dimensions' correct name. Here are some that are usually essential in most ETLs:

* `ga:dateHourMinute` - date and time with minute precision
* `ga:browser`
* `ga:fullReferrer`
* `ga:pagePath`

### 3. Prepare a list of custom dimensions

If you have those in your GA, a full list can be found in **GA Admin ➔ Property ➔ Custom Definitions ➔ Custom Dimensions**. They will have generic names as `ga:dimension17`.


### 4. Organize your dimensions

For each GA default and custom dimension, think about a target SQL column name. For example `ga:dateHourMinute` will map to `utc_datetime` column name, or my `ga:dimension12` will map to `client_ssn`.

Some dimensionas will need special handling, such as `ga:dateHourMinute`, that will be transformed to correct UTC date and time by `GAAPItoDB` class. Custom dimensions that require custom transformations can be handled by your methods when you extend the `GAAPItoDB` class.

Define which of your dimensions are key. Which combination of dimensions define a row uniquely. We'll need this later when configuring the ETL.

### 5. Prepare a target database

Any database with SQLAlchemy support should work. Install proper SQLAlchemy drivers for your database and think about a correct DB URL. I only tested this class with MariaDB.

Database operations executed by GAAPItoDB are very simple. The target database is queried in the begining of the process just to know what was the hit time of the last record written to database.

When data is ready to be written, INSERTS will be done efficiently by Panda's `to_sql` method using a SQLAlchemy connection. If target table doesn't exist, it will be created by `to_sql` with column types with optimal types.

Indexes to speed up queries will not be created. Create them manually when you already have data in the table.

Set correct database access permissions. We'll need SQL grants for `INSERT`, `SELECT`, `DROP TABLE` (if `incremental` is `False` or `restart` is `True`) and `CREATE TABLE` (on first run or if `restart` is `True`).

### 6. Figure out the maximum number of days GA will deliver unsample data for your dimension set

When found, put this number of days in `dateRangePartitionSize` class constructor parameter. More days will make it run faster and require less API calls, but increase your chance of getting sampled (incomplete) data. Less days ensures complete data but makes more API calls and takes longer times to run.

There are 2 ways to get the optimal number of days.

1. Go to _GA Custom Reports_ and build a flat table custom report with your desired dimension set. Set a large period, say 2 entire weeks or 14 days, and check if the sampling flag close to report title is yellow (sampled data, bad) or green (unsampled and complete data). Reduce number of days until it becomes green. Lets say you found 4 days GA will deliver unsampled data, then remove 1 or 2 more days just to completely reduce completely your chance of getting sampled data. So use **2** days in this example.

2. Prepare your ETL with this class, put a reasonable large number in `dateRangePartitionSize`, run the ETL in debug mode (see `examples/etl.py`) and check output. The class will warn you if it is getting sampled data. Then reduce the number of days until you get only unsampled data.

### 7. Determine the Start and maybe the End date that your dimensions have data

GA API will deliver no data if one single dimension has no data for that period. Although `GAAPItoDB` class is smart and will get all dimensions separately in sub reports and then join them through the key dimensions, don't waste API calls and time with out of boundaires periods.

### 8. Configure your dimensions for the class

The `dimensions` class constructor needs to get a list of dimensions and their configurations. Such as:

```python
dimensions = [
            {
                'title': 'utc_datetime',
                'name': 'ga:dateHourMinute',
                'type': 'datetime',
                'synccursor': True,
                'key': True,
                'sort': True,
                'keeporiginal': True
            },
            {
                'title': 'sucursal_apolice',
                'name': 'ga:dimension7',
                'key': True,
                'keeporiginal': True,
                'transform': self.BSExplodeAndSplit, # (report, this_dimension)
                'transformspawncolumns': ['sucursal','apolice'],
                'transformspawncolumnstypes': ['int','int'],
                'transformparams': {
                    'explodeseparators': [','],
                    'splitseparators': ['-'],
                }
            },
            {
                'title': 'corretor_sucursal',
                'name': 'ga:dimension11',
                'type': 'int'
            },
            {
                'title': 'corretor_cpfcnpj_session_zeropad_sha256',
                'name': 'ga:dimension20',
                'transform': self.BSNullifyStrings, # (report, this_dimension)
                'transformparams': {
                    'nullify': ['Cookie não definido']
                },
            },
            {
                'title': 'event',
                'name': 'ga:eventLabel',
            }
        ]
```
This is a list of 5 dimensions that will result in 6 SQL columns. This example was extracted from `examples/GABradescoSegurosToDB.py`. See this file for more information.

* `title` - The SQL target column name
* `name` - Google Analytics dimension name
* `type` - If defined, provides a hint for the class on how to transform and optimize this data. Currently supported `datetime` and `int`.
* `synccursor` - If `True`, this database columns will be checked to determine since when data must be grabbed from GA. Usually it is a `datetime` column.
* `key` - If `True`, this is one of the dimensions that composes a single unique row. Usually you'll have several key dimensions.
* `keeporiginal` - Transform data but keep original in an `*_org` column.
* `transform` - A custom function that will be called to transform original data into something new. This function gets a Pandas DataFrame and the dimension structure as parameters to operate.
* `transformspawncolumns` - Tells the class and this specific transform function of this example that this dimension must be split into new columns with these names (2 in this case).
* `transformparams` - An object with information relevant to the `trnsform` function.

### 9. Use a GA View configured with UTC timezone

To avoid time inconsistencies, the module always converts and stores time as UTC in the database. But GA documentation is unclear about how they handle time in days entering and leaving Daylight Savings Time.

So a bet practice is to create a View configured to UTC as timezone, this way time is ensured to be always linear. Not to mention that no timezone conversion will be needed.

### 10. Run regularly with CRON

Once configured, easiest way to use it is with a crontab. I have this on my crontab:

```shell
30 */2 * * * cd ~/bin && ./etl.py
```

Which will run a sync every 2 hours plus 30 minutes. Change it to `@hourly` to get more recent updates.

## Prepare Google Analytics for optimal ETLs

Google Analytics as a UI uses some private unaccessible data to make all its data meaningful. In the API or custom reports level we don't have some very important control data to glue together all dimensions that we can extract.

For example, GA time precision is as bad as 1 minute. So in one single minute you may have many different actions from one single user and you won't even know in which order the user did it. Other limitations are lack of information about session ID, client ID and user ID, all things that help uniquely identify a hit.

To overcome this limitations, create the following custom dimensions in your GA. The code below is just algorithmic ideas that must be executed in the user's browser, you'll have to convert it to real code. For example, the `hash()` function in my examples is anything in the user browser capable of generating a unique {hex|base64|b85} digest. You may want to use [SHA-256 impementation of `SubtleCrypto.digest()`](https://developer.mozilla.org/en-US/docs/Web/API/SubtleCrypto/digest) available on most modern browsers. SHA-3 or SHAKE would be smaller and more efficient, but only SHA-256 is apparently widely implemented right now. And wherever you go here, do not use SHA-1.

### Custom dimension `ClientID`
([reference](https://stackoverflow.com/a/20054201/367824))
```javascript
ClientID = ga.clientID
```

### Custom dimension `UserID`
Maybe you'll want `UserID` to be simply your `BusinessUserID`, or maybe you want to differentiate same user across different devices. I prefer second option.
```javascript
UserID = hash(BusinessUserID + ClientID)
```
### Custom dimension `SessionID`
```javascript
if (user_just_logged_in == true) {
    SessionID = hash(UserID + Date.now());
    ga.setNewSession()
}
```

### Custom dimension `SequenceID`
This is just a sequential number, doesn't have to be a unique identifier as the others, thats why its just browser's time as an integer. But compound key `(SessionID, SequenceID)` is globaly unique.

```javascript
SequenceID = Date.now()    /* milliseconds resolution */
```

### Custom dimension `HitID`
Something to identify uniquelly and globaly a single user action
```javascript
HitID = hash(SessionID + Date.now())
```