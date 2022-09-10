# Basic ETL

You can use this project structure for a simple ETL process. In this example, I extract weather data about some states in Australia, transform the data, and load the data into a postgresql database.

<br/>

# The Structure

## The ```helpers``` directory
The ```helpers``` directory contains ```helpers.py``` which contains all helper functions. In future, these functions can be categorised into various subjects.

<br/>

## ```py_etl``` 
This directory contains all extract, load, and transform logic. New functions can be added to the ```extract.py```, ```transform.py```, and ```load.py``` scripts after testing. These modules can also be grouped by subject area in the future.

<br/>

## ```tests```
This directory contains all tests for the project. Each test class must be context-specific i.e. all tests for the weather pipeline should have their own group of tests (test class). I strongly suggest using TDD in your pipeline development. Each function should be tested before it's implemented in any pipeline. This can be easily achieved by writing tests as you build the application. Tests pay dividends in the long run.

<br/>

## ```pipelines```
All pipelines are stored in this directory. Pipeline files should be context specific, for e.g. the weather pipeline is created in the ```weather_pipeline.py``` file.


<br/>


# Todo 
- Logging
