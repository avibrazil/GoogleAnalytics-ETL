# Google Analytics ETL

A python base class to extract and continuously sync data from Google Analytics, and write it to a SQL database.

It works similarly to Goole Analytics Custom Reports where you define a list of dimensions to be displayed in a flat table. But doesn't have the limitations of GA's UI. In addition, data can be custom transformed and will be written to a SQL database.

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

Extracting data from Google Analytics is not easy due to its variety and quantity of data, specially if you have a busy site with custom dimensions (GA Admin ➔ Property ➔ Custom Definitions ➔ Custom Dimensions).

So use the example file `examples/GABradescoSegurosToDB.py`. Copy it to a new place and edit it to create your own concrete classes with desired dimensions.

Then copy `examples/etl.py` and edit it to call your custom classes. This will be the executable script that you'll run every time.

### With CRON

Once configured, easiest way to use it is with a crontab. I have this on my crontab:

```shell
@daily    python3 /home/avi/bin/etl.py
```

Which will run a sync daily. Change it to `@hourly` to get more recent updates.
