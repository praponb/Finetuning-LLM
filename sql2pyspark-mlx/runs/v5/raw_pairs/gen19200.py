from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L2", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l2"),
        F.avg("salary").alias("avg_salary"),
    )
    .filter(F.col("avg_salary") >= 106000)
    .select("is_remote", "pct_l2", "avg_salary")
    .orderBy(F.col("pct_l2").desc())
)
