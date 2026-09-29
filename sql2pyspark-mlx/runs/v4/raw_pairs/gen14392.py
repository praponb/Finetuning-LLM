from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("is_remote") & F.col("dept_id").isin([15, 17, 38, 40]) & F.col("manager_id").isin([4, 12, 33]))
)

summary = (
    base
    .groupBy("level", "manager_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L5", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l5"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = (
    summary
    .filter(F.col("pct_l5") > 53)
    .select("level", "manager_id", "pct_l5", "stddev_salary")
)
