from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNull() | F.col("job_title").isin(["Director", "Engineer"]))
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
)
