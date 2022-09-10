# First ETL  

There might be a time when your business wants to perform some very basic ETL for a small dataset. You can use the project structure in your project.

<br/>

# The Structure

## The ```helpers``` directory
The ```helpers``` directory contains ```helpers.py``` which contains all helper functions. In future, these functions can be categorised into various subjects.

<br/>

## ```py_etl``` 
This directory contains all extract, load, and transform logic. New functions can be added to the ```extract.py```, ```transform.py```, and ```load.py``` scripts after testing.

<br/>

## ```tests```
This directory contains all tests for the project. I strongly suggest using TDD. Each function should be tested before it's implemented in any pipeline. Tests pay dividends in the long run.

<br/>

## ```pipelines```
All pipelines are stored in this directory. Pipelines files should be context specific, for e.g. the weather pipeline is created in the ```weather_pipeline.py``` file.


<br/>


# Todo 
- Logging
