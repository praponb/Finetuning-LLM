from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id", "manager_id")
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_rows"),
        F.percentile_approx("salary", 0.5).alias("salary_p50"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_rows"),
        F.sum(F.when(F.col("level") == "L3", F.col("salary")).otherwise(0)).alias("l3_salary"),
    )
)
