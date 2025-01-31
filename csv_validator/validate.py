'''
CMD tool for validating CSV file content and generating data quality reports
'''

import os
import logging
import json
import argparse

from typing import Annotated, Literal, List, Union, Tuple, Generator
from datetime import datetime
from urllib.parse import urlparse

import pydantic
import boto3
import s3fs
import polars as pl

# ENV VARS
AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
# Constants for process results

PRESIGNED_URL_DURATION = 60 * 60

# S3 Client
S3_CLIENT = boto3.client("s3")

# Logger configuration
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s ")
LOGGER = logging.getLogger("csv-validator")
LOGGER.setLevel(os.getenv("LOGLEVEL", "DEBUG"))

class Jobstate:
    success = 'success'
    warning = 'warning'
    failure = 'failure'

class ValidationError(ValueError):
    pass

def parse_s3_path(path: str) -> Tuple[str, str]:
    """
    S3 path parser
    :param path: S3 path to parse (s3://bucket/path/to/file.csv)
    :return: Bucket name, Key
    """
    parts = urlparse(path)
    LOGGER.info("Bucket: %s - Key: %s", parts.netloc, parts.path.lstrip("/"))
    return parts.netloc, parts.path.lstrip("/")

def put_results_s3(data: pl.DataFrame, task_config: dict) -> bool:
    """
    Writes a CSV file to an Amazon S3 bucket
    :param data: pl.Dataframe
    :param task_config: dict
    :return: True if src_data was added to s3_bucket, otherwise False
    """
    
    LOGGER.info("Loading result CSV file to S3...")

    try:
        execution_starttime = datetime.strptime(
            task_config['execution_starttime'][:10], '%Y-%m-%d')
        log_path = (
            f"ValidationResult")
        source_file = parse_s3_path(task_config["source"])[1]
        s3_key_log = (
            f'{log_path}'
            f'{execution_starttime.strftime("/%Y/%m/%d/")}'
            f'{source_file[:source_file.find('.')]}_result.csv'
        )
        s3_bucket_log = parse_s3_path(task_config["log_bucket_name"])[0]
        LOGGER.debug("Results to be stored in: s3://{}/{}".format(s3_bucket_log, s3_key_log))
        
        # sts_client = boto3.client('sts')
        # response = sts_client.assume_role(
        #     RoleArn='arn:aws:iam::891377129971:role/A3AllowRole',
        #     RoleSessionName='TestSession'
        # )
        # credentials = response['Credentials']
        # access_key = credentials['AccessKeyId']
        # secret_key = credentials['SecretAccessKey']
        # session_token = credentials['SessionToken']
        # fs = s3fs.S3FileSystem(key=access_key, secret=secret_key, token=session_token)
        fs = s3fs.S3FileSystem()
        destination = f's3://{s3_bucket_log}/{s3_key_log}'
        with fs.open(destination, 'wb') as f:
            data.write_csv(f)
            LOGGER.info(f'Result CSV file successfully uploaded on {destination}')
        return True
    except Exception as e:
        LOGGER.error(e)
        return False

def add_result_frame_row(row_num=None, invalid_val=None, rule=None, blocking=False) -> pl.LazyFrame:
    '''
    Add a row to the result dataframe
    :param row_num: Row number
    :param invalid_val: Invalid value
    :param rule: Rule that was violated
    :param blocking: Flag to indicate if the rule is blocking
    '''
    schema = pl.Schema([("row_num", pl.Int32), ("invalid_values", pl.String), ("rule", pl.String), ("is_blocking", pl.Boolean)])
    if rule is None:
        return pl.LazyFrame(schema=schema)
    else:
        return pl.LazyFrame(schema=schema).select(
                        pl.lit(row_num).alias('row_num'),
                        pl.lit(invalid_val).alias('invalid_values'),
                        pl.lit(rule).alias('rule'),
                        pl.lit(blocking).alias('is_blocking'))

def initialize_dataframe(
        file_url,
        delimiter: str = ',',
        check_col_count: bool = True,
        col_count_blocking: bool = False
        ) -> Tuple[pl.LazyFrame, List[List[str]]]:
    '''
    Initialize the dataframe from the file available on file_url
    :param file_url: URL of the file to be read
    :param delimiter: Delimiter used in the file, default is \',\'
    :param check_col_count: Flag for column count check. Default is True
    :param col_count_blocking: Flag whether column count is blocking. Default is False
    '''
    err = add_result_frame_row(None, None, None, False)

    if check_col_count:
        #In case the file content is correct and there are no "ragged" lines,
        # i.e lines with more or less columns than the header the df creation should be successful
        if "s3://" in file_url:
            session = boto3.session.Session()
            credentials = session.get_credentials().get_frozen_credentials()
            storage_options = {
                "aws_access_key_id": credentials.access_key,
                "aws_secret_access_key": credentials.secret_key,
                "aws_region": AWS_REGION,
            }
        else:
            storage_options = None
        try:
            df = pl.scan_csv(file_url, separator=delimiter, infer_schema=False, low_memory=True, storage_options=storage_options)
        except pl.exceptions.ComputeError as e:
            err_msg = str(e).split('\n', maxsplit=1)[0].capitalize()
            LOGGER.warning(
                "Encountered the following error: %s\n Reading with truncate_ragged_lines=True",
                err_msg
                )
            err = add_result_frame_row(None, None, 'column_count', col_count_blocking)
            try:
                df = pl.scan_csv(
                    file_url,
                    separator=delimiter,
                    infer_schema=False,
                    truncate_ragged_lines=True,
                    storage_options=storage_options
                    )
                LOGGER.debug(
                    "Dataframe successfully created with truncate_ragged_lines=True."
                    )
            except Exception as e:
                LOGGER.fatal("Error reading file: %s", file_url)
                raise e
        except FileNotFoundError:
            raise FileNotFoundError(f'File {file_url} not found')
        except Exception as e:
            LOGGER.fatal("Error reading file: %s. Reason: %s", file_url, e)
            raise e
    else:
    # In case there is no column count check in the configuration
    # the df is read with truncate_ragged_lines=True on the first try
    # to make sure a dataframe is created and further validation can be processed.
        try:
            df = pl.scan_csv(
                file_url,
                separator=delimiter,
                infer_schema=False,
                truncate_ragged_lines=True,
                low_memory=True,
                storage_options=storage_options
                )
            LOGGER.debug(
                "Dataframe successfully created with truncate_ragged_lines=True."
                )
        except FileNotFoundError:
            raise FileNotFoundError(f'File {file_url} not found')
        except Exception as e:
            LOGGER.fatal(f'Error reading file: {file_url}')
            raise e
    return df, err

def header_checker(
        header_list: list,
        df_headers: list,
        header_blocking: bool = False
        ) -> pl.LazyFrame:
    '''
    This function checks for possible header errors (e.g extra header, wrong order, typo, etc.)
    :param header_list: List of headers from the configuration
    :param df_headers: List of headers from the dataframe
    :param header_blocking: Flag to indicate if the header check is blocking. Default is False
    '''
    blocking = header_blocking
    if header_list is None or df_headers is None:
        raise ValueError('Header list and file headers cannot be None')
    LOGGER.debug(f'Header list from configuration: {"|".join(header_list)}')
    LOGGER.debug(f'Header list from the file: {"|".join(df_headers)}')
    # The fist check will simply indicate us that the 2 lists are different.
    # Further tests are done to decide the exact problem with the header.
    if header_list != df_headers:
        if set(header_list) == set(df_headers) and len(header_list) == len(df_headers):
            LOGGER.warning('Headers are the same but in a different order')
            err_list = add_result_frame_row(None, 'Incorrect column order', 'header', blocking)
        elif set(header_list) == set(df_headers) and len(header_list) != len(df_headers):
            LOGGER.warning('File headers contain duplicate columns')
            err_list = add_result_frame_row(None, 'Duplicate column names', 'header', blocking)
        elif set(header_list).symmetric_difference(set(df_headers)) and len(header_list) == len(df_headers):
            LOGGER.warning('Headers are different')
            err_list = add_result_frame_row(None, ', '.join(set(header_list).symmetric_difference(set(df_headers))), 'header', blocking)
        elif set(header_list).symmetric_difference(set(df_headers)) and len(header_list) != len(df_headers):
            if set(header_list).issubset(set(df_headers)):
                LOGGER.warning('Extra columns in the file')
                err_list = add_result_frame_row(None, ', '.join(set(df_headers).difference(set(header_list))), 'header', blocking)
            elif set(df_headers).issubset(set(header_list)):
                LOGGER.warning('Missing columns in the file')
                err_list = add_result_frame_row(None, ', '.join(set(header_list).difference(set(df_headers))), 'header', blocking)
            else:
                LOGGER.warning('Multiple header errors')
                err_list = add_result_frame_row(None, ', '.join(set(header_list).symmetric_difference(set(df_headers))), 'header', blocking)
        else:
            LOGGER.warning('Unknown header error')
            err_list = add_result_frame_row(None, ', '.join(set(header_list).symmetric_difference(set(df_headers))), 'header', blocking)
    else:
        LOGGER.info('No header errors')
        err_list = add_result_frame_row(None,None,None,False)

    return err_list

def file_level_validations(df: pl.DataFrame, header_list: list, rules: dict) -> List[List[str]]:
    '''Perform global validations on the file that are not covered by pydantic
    :param df: The file content as a polars dataframe
    :param header_list: List of headers from the configuration
    :param rules: Dictionary containing the rules for the validations'''

    err_list = add_result_frame_row(None, None, None, False)
    col_names = df.collect_schema().names()
    for rule in rules["file_rules"]:

        blocking = True if rule['is_blocking'] else False
        if rule["rule"] == 'record_count':
            LOGGER.debug(f'Checking record count. Expected between {rule["min"]} and {rule["max"]} records')
            rec_count = df.select(pl.len()).collect().item()
            if rec_count < int(rule["min"]) or rec_count > int(rule["max"]):
                LOGGER.warning(f'Expected between {rule["min"]} and {rule["max"]} records. Actual: {len(df)}')
                err_list = pl.concat([
                    err_list, 
                    add_result_frame_row(None, f'Expected between {rule["min"]} and {rule["max"]} records. Actual: {rec_count}', 'record_count', blocking)])
        elif rule["rule"] == 'column_count':
            LOGGER.debug(f'Checking column count. Expected {len(header_list)} columns')
            if len(col_names) != len(header_list):
                LOGGER.warning(f'Expected {len(header_list)} columns, got {len(df.columns)}')
                err_list = pl.concat([err_list, add_result_frame_row(None, f'Expected {len(header_list)} columns, got {len(df.columns)}', 'column_count', blocking)])
    if any((True for _ in rules["column_rules"] if _["rule"] == "UNIQUE")):
        for rule in rules["column_rules"]:
            if rule["rule"] == 'UNIQUE':
                blocking = True if rule['is_blocking'] else False
                if rule["column_name"] in col_names:
                    LOGGER.debug(f'Checking for unique values in column {rule["column_name"]}')
                    if df.select(pl.col(rule["column_name"]).hash().is_duplicated()).collect(streaming=True).select(pl.any(rule["column_name"])).item():
                        LOGGER.warning(f'Column {rule["column_name"]} contains duplicate values')
                        err_list = pl.concat([err_list, add_result_frame_row(None, f'Column {rule["column_name"]} contains duplicate values', 'unique', blocking)])
                else:
                    LOGGER.warning(f'Column {rule['column_name']} not found in the file')

    return err_list

def validate_datetime_format(value: str, format: str, allow_nulls: bool) -> datetime:
    if value is None:
        if allow_nulls:
            return value
        else:
            raise ValueError('Value cannot be null')
    else:
        try:
            return datetime.strptime(value, format)
        except Exception as e:
            raise ValueError(e)

def generate_base_model(columns: list, df_columns: list, rules: List[dict]) -> dict:
    '''
    Generating the base model for the pydantic model, which will perform the column level validations. For each set of rules there are a version that allows nulls and one that doesn't.
    The result will be a dictionary with the column names as keys and the corresponding pydantic types as values which will serve as input for the pydantic.create_model function.
    :param columns: List of columns from the configuration
    :param df_columns: List of columns from the file/dataframe
    :param rules: List of rules from the configuration
    '''
    fields = {}
    LOGGER.debug(f'Building base model for validation')
    for column in columns:
        if column in df_columns:
            allow_nulls = False if any([True for _ in rules if _["rule"].casefold() == "null" and _["column_name"] == column]) else True
            fields[column] = (Union[str, None], ...) if allow_nulls else (str, ...)
            for rule in rules:
                if rule['column_name'] == column:
                    if rule['rule'].casefold() == 'email':
                        fields[column] = (Union[pydantic.EmailStr, None], ...) if allow_nulls else (pydantic.EmailStr, ...)
                        LOGGER.debug(f'Adding {rule['rule']} validation for column {column}. Configuration: {fields[column]}')
                    elif rule['rule'].casefold() == 'numeric':
                        fields[column] = (Union[Annotated[float, pydantic.Field(strict=False)], None], ...) if allow_nulls else (Annotated[float, pydantic.Field(strict=False)], ...)
                        LOGGER.debug(f'Adding {rule['rule']} validation for column {column}. Configuration: {fields[column]}')
                    elif rule['rule'].casefold() == 'date':
                        date_format = rule['format']
                        fields[column] = (Annotated[datetime, pydantic.BeforeValidator(lambda v: validate_datetime_format(v, date_format, allow_nulls))], ...)
                        LOGGER.debug(f'Adding {rule['rule']} validation for column {column}. Configuration: {fields[column]}')
                    elif rule['rule'].casefold() == 'lov':
                        fields[column] = (Union[Literal[tuple(rule['values'])], None], ...) if allow_nulls else (Literal[tuple(rule['values'])], ...)
                        LOGGER.debug(f'Adding {rule['rule']} validation for column {column}. Configuration: {fields[column]}')
                    elif rule['rule'].casefold() == 'length':
                        fields[column] = (Union[Annotated[str, pydantic.Field(min_length=int(rule['column_length']['min']), max_length=int(rule['column_length']['max']))], None], ...) if allow_nulls else (Annotated[str, pydantic.Field(min_length=int(rule['column_length']['min']), max_length=int(rule['column_length']['max']))], ...)
                        LOGGER.debug(f'Adding {rule['rule']} validation for column {column}. Configuration: {fields[column]}')
                    elif rule['rule'].casefold() == 'range':
                        fields[column] = (Union[Annotated[float, pydantic.Field(ge=int(rule['column_value_range']['min']), le=int(rule['column_value_range']['max']), strict=False)], None], ...) if allow_nulls else (Annotated[float, pydantic.Field(ge=int(rule['column_value_range']['min']), le=int(rule['column_value_range']['max']), strict=False)], ...)
                        LOGGER.debug(f'Adding {rule['rule']} validation for column {column}. Configuration: {fields[column]}')
                    elif rule['rule'].casefold() in ['unique', 'null']:
                        continue
                    else:
                        fields[column] = (Union[str, None], ...) if allow_nulls else (str, ...)
                        LOGGER.debug(f'Adding default validation for column {column} as rule name didn\'t match any rules. Configuration: {fields[column]}')
                else:
                    continue
        else:
            #If a column is not in the file it doesn't make sense to add it to the model regardless of the fact there is a validation rule in the dictionary.
            #A warning is logged, but an error like this will also be caught by the header check or the file level check.
            LOGGER.warning(f'Column {column} not found in the file. Available columns: {df_columns}')
    return fields

def create_dataframe_validator_model(columns: list, df_columns: list, rules: List[dict]) -> pydantic.BaseModel:
    '''
    Wrapper function that calls the generate_base_model function adds creates a "DataFrameValidator" around it
    :param columns: List of columns from the configuration
    :param df_columns: List of columns from the file/dataframe
    :param rules: List of rules from the configuration
    '''
    fields = generate_base_model(columns, df_columns, rules)
    # Generate the dynamic model
    DynamicModel = pydantic.create_model('model', **fields)

    # Create the dataframe_validator model
    DataframeValidatorModel = pydantic.create_model(
        'dataframe_validator',
        df_dict=(List[DynamicModel], ...)
    )
    LOGGER.debug(f'Pydantic model successfully created')
    # Assign the dynamic model to the global namespace. This is required for the process_batch function to work in parallel mode.
    globals()['dataframe_validator'] = DataframeValidatorModel
    return DataframeValidatorModel

def batch_generator(df: pl.LazyFrame, df_height: int, batch_size: int) -> Generator[pl.LazyFrame, None, None]:
    '''
    Generator that yields batches of the dataframe
    :param df: The dataframe to be batched
    :param batch_size: The size of the batches
    '''
    for i in range(0, df_height, batch_size):
        yield df.slice(i, batch_size)

def filter_column_rules(rules: List[dict], column_name: str) -> dict:
    '''
    Filter the column rules based on the column name
    :param rules: List of rules from the configuration
    :param column_name: The name of the column
    '''
    return [rule for rule in rules if rule['column_name'] == column_name]

def process_batch(batch: pl.LazyFrame, model: Union[pydantic.BaseModel, bytes], column_rules: list) -> Union[None, List[List[str]]]:
    '''
    Processing the batch using the pydantic model. The batch will come either from the sequential or parallel route. Coming from the parallel route the model will be deserialized using cloudpickle,
    therefore the model will be a byte string that should be serialized back to a pydantic model.
    :param batch: The batch to be processed as a polars dataframe
    :param model: The pydantic model to be used for the validation as a pydantic model or a byte string
    '''
    err = add_result_frame_row(None, None, None, False)
    try:
        model(df_dict=batch.collect().to_dicts())
    except pydantic.ValidationError as e:
        for error in e.errors():
            blocking = filter_column_rules(column_rules, error['loc'][2])[0]['is_blocking']
            if error['input'] is None:
                row_num = batch.filter(pl.col(error['loc'][2]).is_null()).select(pl.col('row_num')).collect().item()+2
                rule = 'null'
                err = pl.concat([err, add_result_frame_row(row_num, f'Column {error['loc'][2]} contains NULLs', rule, blocking)])
            else:
                row_num = batch.filter(pl.col(error['loc'][2]) == error['input']).select(pl.col('row_num')).collect().item()+2
                rule = filter_column_rules(column_rules, error['loc'][2])[0]['rule']
                err = pl.concat([err, add_result_frame_row(row_num, error['input'], rule, blocking)])
        return err
    return err

def file_validate_start(task_config: dict) -> pl.DataFrame:
    '''The main function that orchestrates the validation process. It will read the file, perform the file level validations, create the pydantic model and process the batches.
    :param task_config: The configuration for the validation process'''
    
    # example s3 = s3://example_bucket/files/csv/example.csv
    if "s3://" in task_config["source"]:
        s3_path = parse_s3_path(task_config["source"])

        s3_bucket = s3_path[0]
        s3_key = s3_path[1]

        LOGGER.info("S3 Key: %s" % s3_key)
        LOGGER.debug(f'S3 Bucket: {s3_bucket}')
        session = boto3.session.Session()
        credentials = session.get_credentials().get_frozen_credentials()
        storage_options = {
            "aws_access_key_id": credentials.access_key,
            "aws_secret_access_key": credentials.secret_key,
            "aws_session_token": credentials.token,
            "aws_region": AWS_REGION,
        }
        file_url = f's3://{s3_bucket}/{s3_key}'
        #Seems like polars have a problem with presigned urls. This issue is supposed to be fixed, however I still get bad request on 1.20
        #https://github.com/pola-rs/polars/issues/18186
    
        # params = {"Bucket": s3_bucket, "Key": s3_key}
        # file_url = S3_CLIENT.generate_presigned_url(
        #     "get_object", Params=params, ExpiresIn=PRESIGNED_URL_DURATION
        # )
        # LOGGER.debug(f'S3 file URL: {file_url}')
    else:
        file_url = task_config["source"]
    
    delimiter = task_config["file"]["delimiter"]
    header_list = task_config["file"]["header_str"].split(delimiter)
    check_col_count = any([True for _ in task_config["validate"]["file_rules"] if _["rule"] == "column_count"])
    if check_col_count:
        col_count_blocking = True if any([True for _ in task_config["validate"]["file_rules"] if _["rule"] == "column_count" and _["is_blocking"]]) else False
    else:
        col_count_blocking = False
    batch_size = task_config['batch_size']
    df, errors = initialize_dataframe(file_url, delimiter, check_col_count, col_count_blocking)

    if task_config["file"]["header_flag"]:
        header_blocking = True if any(True for _ in task_config["validate"]["file_rules"] if _["rule"] == "header" and _["is_blocking"]) else False
        errors = pl.concat([errors, header_checker(header_list, df.collect_schema().names(), header_blocking)])

    if task_config["validate"]["file_rules"]:
        errors = pl.concat([errors, file_level_validations(df, header_list, task_config["validate"])])

    #Since the column level checks are the most resource intensive, they are only performed if there are any rules in the configuratio (other than the UNIQUE check as that is validated on the dataframe level).
    if any(True for _ in task_config["validate"]["column_rules"] if _["rule"] != "UNIQUE"):
        data_frame_validator = create_dataframe_validator_model(header_list, df.collect_schema().names(), task_config["validate"]["column_rules"])
        
        dfh = df.collect().height
        batches = batch_generator(df, dfh, batch_size)
        i = 0
        for batch in batches:
            errors = pl.concat([
                errors, 
                process_batch(
                    batch.with_row_index('row_num', offset=i*batch_size), 
                    data_frame_validator,
                    task_config['validate']['column_rules'])])
            i += 1

    return errors.collect()

def get_args():

    p = argparse.ArgumentParser('in_validate')
    p.add_argument(
        '--cfg',
        type=str,
        help='Config file in json format')
    p.add_argument(
        '--file_path',
        type=str,
        help='File path to source csv. Overwrites path in config')
    p.add_argument(
        '--batch_size',
        type=int,
        help='Batch size for file processing. Overwrites default size in config (10000)')
    return p.parse_args()

def main():
    args = get_args()
    if args.cfg:
        with open(args.cfg, 'r', encoding='utf8') as f:
            event = json.load(f)
    else:
        LOGGER.fatal('No event file provided')
        raise ValueError('Please provide a valid configuration json.')
    if args.file_path:
        event['task_config']['source'] = args.file_path
    if args.batch_size:
        event['task_config']['batch_size'] = args.batch_size
    task_config = event["task_config"]

    try:
        task_config['execution_starttime'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
        validation_result = file_validate_start(task_config)
        if validation_result.is_empty():
            LOGGER.info("Validation successful. No errors detected.")
            jstate = Jobstate.success
        # Check "warning" state
        warn = not(validation_result.filter(pl.col('is_blocking') == False).is_empty())
        # Check "error" state
        err = not(validation_result.filter(pl.col('is_blocking') == True).is_empty())
        
        if warn and not err:
            LOGGER.warning("Validation successful with warnings.")
            jstate = Jobstate.warning
        elif err:
            LOGGER.error("Validation failed.")
            jstate = Jobstate.failure
        if jstate == 'success':
            validation_result = add_result_frame_row(None, 'No errors, validation successful', None, False)
        if "s3://" in task_config['log_bucket_name']:
            put_results_s3(validation_result, task_config)
        elif task_config['log_bucket_name'] != '':
            LOGGER.info('Writing validation results to local file')
            try:
                validation_result.write_csv(task_config['log_bucket_name'])
            except Exception as e:
                LOGGER.error(f'Error writing to local file: {e}')
            LOGGER.info('Validation results written to local file')
        execution_endtime = datetime.now()
        time_elapsed = execution_endtime - datetime.strptime(task_config['execution_starttime'], "%Y-%m-%dT%H:%M:%S.%f")
        LOGGER.info("[Validation]: " "Time elapsed (hh:mm:ss.ms) {}".format(time_elapsed))
        
    except Exception as e:
        LOGGER.fatal("Validation failed with unexpected exception")
        unknown_exc = e

    finally:
        if 'unknown_exc' in locals():
            execution_endtime = datetime.now()
            time_elapsed = execution_endtime - datetime.strptime(task_config['execution_starttime'], "%Y-%m-%dT%H:%M:%S.%f")
            LOGGER.info("[Validation]: " "Time elapsed (hh:mm:ss.ms) {}".format(time_elapsed))
            raise unknown_exc
        else:
            if jstate == 'failure':
                execution_endtime = datetime.now()
                time_elapsed = execution_endtime - datetime.strptime(task_config['execution_starttime'], "%Y-%m-%dT%H:%M:%S.%f")
                LOGGER.info("[Validation]: " "Time elapsed (hh:mm:ss.ms) {}".format(time_elapsed))
                raise ValidationError("Validation failed")
    
if __name__ == '__main__':
    main()
