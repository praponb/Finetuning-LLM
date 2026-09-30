from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L2")
    .groupBy(F.quarter("hire_date").alias("hire_quarter"), "is_remote")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.sum("salary").alias("total_salary"),
    )
    .filter(F.col("total_salary") >= 625000)
    .select("hire_quarter", "is_remote", "level_values", "total_salary")
)
