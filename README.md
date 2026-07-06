# Data-Engineering-Technical-Test-Ominomo

## Introduce 

This project is a dynamic framework for processing motor insurance policy data. The core philosophy of this solution is to avoid ad-hoc scripting and instead provide a dynamic engine that executes data pipelines based on a provided configuration (metadata).

## Technologies

Python 

Docker (always runs under the same conditions inside the container, regardless of the machine it is executed on)

PySpark (Big Data)

## Implementation

The application initializes a local SparkSession within the container environment
Then, it goes through sources, transformations, and sinks in order.
A dfs (DataFrames) dictionary was created to store all the necessary data for transformations and output.

### Sources

It loops through all sources, reads the files from the given path with the given format and writes them into the dfs dictionary (DataFrame type).

### Transformations

It loops through all transformations, checks the type and processes further depending on it:

validate_fields: For each field, we loop through every rule. If a rule is not met, it is later added to validation_ko with all saved error logs; otherwise, it goes to validation_ok if there are no errors.

add_fields: For each function, we check which one it is (e.g., current_timestamp) and execute it on the corresponding input DataFrame.
At the end, all these changes are saved in the dfs dictionary so we can use them for the output.

### Sinks
For each input from sinks found in the dfs dictionary, we get the corresponding DataFrame and save the file with the given format, mode, and location.

### Note
I slightly modified the input JSON. I changed the paths to the input and output files to make them more intuitive for me.
Also, inside validations, it said "validations", so I changed it to "rules" there to make it easier to differentiate.

## Run
Builds the image (with dockerfile):
docker build -t ominimo-test .

Uses that built image (without dockerfile):
docker run -v "${PWD}/data:/app/data" ominimo-test


