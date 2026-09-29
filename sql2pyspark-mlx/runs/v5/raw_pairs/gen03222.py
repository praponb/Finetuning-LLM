from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Marketing", 1)), F.lit(0)), 2).alias("budget_per_marketing"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("budget_per_marketing") >= 0)
    .orderBy(F.col("budget_per_marketing").asc_nulls_last())
)
