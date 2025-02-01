# CSV validator
## Description
This project is designed to validate CSV files based on a set of predefined rules. It uses libraries such as polars and pydantic to perform validations efficiently. This was a work project originally, but in that version pydantic was ditched due to memory issues.
I thought to make this available as a public repository to showcase some interesting things I learned while working on this. It is nowhere near a finished tool, but it can does what it supposed to do.

## Installation
1. **Clone the repository:**
   ```sh
    git clone https://github.com/kferenc3/validate-public
    cd validate-new
    ```
2. **Install dependencies:**
  This project uses [Poetry](https://python-poetry.org/docs/) for dependency management. Ensure you have Poetry installed, then run:
    ```sh
      poetry install
    ```
## Usage

### Validating CSV Files
To validate the CSV files, you can run the `csv_validator/validate.py` script. The script requires a config file in json format to run. It is possible to read csv files from S3 as well and results can be written either locally or to a specified S3 location.

### Example Command
```sh
python csv_validator/validate.py --cfg <config.json>
```
#### With Poetry
```sh
poetry run python csv_validator/validate.py --cfg <config.json>
```

## Configuration
The config json consist of 3 main subdicts: file, file_rules and column_rules. Apart from them there are a few additional entries that are required: source, log_bucket_name and batch_size. Source and batch_size can be overwritten with `--file_path` and `--batch_size` command line arguments.

## Backlog
- rework existing tests and increase coverage
- improve readme
