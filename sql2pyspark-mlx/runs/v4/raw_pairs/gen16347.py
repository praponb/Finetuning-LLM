from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([15, 19, 39]) & F.col("level").isin(["L5", "L3"]))
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L5", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l5"),
    )
)
