from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("level")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.min("salary").alias("min_salary"),
    )
    .filter(F.col("min_salary") >= 112000)
    .drop("min_salary")
)
