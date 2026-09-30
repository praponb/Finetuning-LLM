from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.percentile_approx("salary", 0.95).alias("salary_p95"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
        F.max("salary").alias("max_salary"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
    )
)
