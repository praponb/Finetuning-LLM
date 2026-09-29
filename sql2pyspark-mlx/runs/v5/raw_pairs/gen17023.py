from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title", "manager_id")
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.avg("salary").alias("avg_salary"),
        F.sum(F.when(F.col("level") == "L1", F.col("salary")).otherwise(0)).alias("l1_salary"),
    )
    .filter(F.col("l1_salary") < 61)
    .drop("l1_salary")
)
