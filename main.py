import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def apply_validations(data_file, validation_list):

    data_file = data_file.withColumn("validation_errors", F.array())

    for v in validation_list:
        field = v['field']
        for rule in v['rules']:
            if rule == "notEmpty":
                cond = (F.col(field) == "") | (F.col(field).isNull())
                err_msg = f"{field} is empty"
            elif rule == "notNull":
                cond = F.col(field).isNull()
                err_msg = f"{field} is null"
            elif rule == "minAge":
                cond = (F.col(field) < 18) | (F.col(field).isNull())
                err_msg = f"{field} must be at least 18"
            else:
                continue

            data_file = data_file.withColumn("validation_errors",
                               F.when(cond, F.array_union(F.col("validation_errors"), F.array(F.lit(err_msg))))
                               .otherwise(F.col("validation_errors"))
                               )
    return data_file


def main(metadata_path):

    spark = SparkSession.builder \
        .appName("OminimoTest") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    with open(metadata_path, 'r') as f:
        meta = json.load(f)

    for flow in meta['dataflows']:
        dfs: dict[str, DataFrame] = {}
        for src in flow['sources']:
            print(f"Reading source: {src['name']}")
            dfs[src['name']] = spark.read.format(src['format']).load(src['path'])

        for trans in flow['transformations']:
            params = trans['params']
            input_df = dfs[params['input']]

            if trans['type'] == "validate_fields":
                print("Applying validations...")
                full_df = apply_validations(input_df, params['validations'])

                dfs["validation_ok"] = full_df.filter(F.size(F.col("validation_errors")) == 0).drop("validation_errors")
                dfs["validation_ko"] = full_df.filter(F.size(F.col("validation_errors")) > 0)

            elif trans['type'] == "add_fields":
                print("Adding metadata fields...")
                res_df = input_df
                for field in params['addFields']:
                    if field['function'] == "current_timestamp":
                        res_df = res_df.withColumn(field['name'], F.current_timestamp().cast("string"))
                    elif field['function'] == "upper":
                        res_df = res_df.withColumn(field['name'], F.upper(F.col(field['name'])))
                    else:
                        continue
                dfs[trans['name']] = res_df

        for sink in flow['sinks']:
            if sink['input'] in dfs:
                print(f"Writing to sink: {sink['name']} -> {sink['path']}")
                dfs[sink['input']].write \
                    .format(sink['format']) \
                    .mode(sink['saveMode']) \
                    .save(sink['path'])

    print("Job finished successfully!")


if __name__ == "__main__":
    main("data/metadata.json")