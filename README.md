# validate-new
## Description
This project is designed to validate CSV files based on a set of predefined rules. It uses libraries such as polars, pydantic, and psutil to perform validations efficiently.

## Installation
1. **Clone the repository:**
   ```sh
    git clone https://github.com/kferenc3/in-validate-rework
    cd validate-new
    ```
2. **Install dependencies:**
  This project uses [Poetry](https://python-poetry.org/docs/) for dependency management. Ensure you have Poetry installed, then run:
    ```sh
      poetry install
    ```
## Usage

### Generating Test Data
To generate test data, run the [datagen.py](https://github.com/kferenc3/in-validate-rework/blob/main/tests/datagen.py) script:
    ```sh
    python tests/datagen.py
    ```
This will create a file with an arbitrary name and number of lines of data of test data.

### Changing Date Format
To change the date format in the generated CSV file, run the [tests/formatchanger.py](https://github.com/kferenc3/in-validate-rework/blob/main/tests/formatchanger.py) script:
    ```sh
    python tests/formatchanger.py
    ```
The input and output files names should be specified along with the input and output formats. This can come handy if we want to test files with different datetime formats.

### Validating CSV Files
To validate the CSV files, you can run the `validate_new/in_validate.py` script. There are a few work in progress functionalities, however plain offline run works now without known issues.
Currently the input file name/path can be specified on line 379 of the script.


### Example Command
```sh
python validate_new/in_validate.py --mode offline --event <event.json>
```
#### With Poetry
```sh
poetry run python validate_new/in_validate.py
```

## Configuration
The script has 2 arguments that should be used while using offline:
`--mode` and `--event`
`--mode` should be `offline` and `--event` should be a path to a valid event json file. An example can be found in the repository.
The validation rules and configurations are defined in this example file. You can modify these rules as per your requirements.
