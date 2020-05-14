import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="GoogleAnalyticsETL",
    version="0.5.5",
    author="Avi Alkalay",
    author_email="avibrazil@gmail.com",
    description="Ingest a set of Google Analytics dimensions and put the data into a SQL database; can be run regularly to sync updates incrementally to DB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/avibrazil/GoogleAnalytics-ETL",
    install_requires=['sqlalchemy','pandas','oauth2client','google-api-python-client','python-dateutil'],
    data_files=[('share/GoogleAnalyticsETL/examples',['examples/GABradescoSegurosToDB.py', 'examples/etl-by-email.py','examples/etl.py','examples/sources.conf.example'])],
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: End Users/Desktop",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
        "Operating System :: POSIX",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Internet"
    ],
    python_requires='>=3.6',
)
