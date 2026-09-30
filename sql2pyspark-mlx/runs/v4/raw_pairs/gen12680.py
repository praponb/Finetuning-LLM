from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4128000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.max("budget").alias("max_budget"),
    )
    .orderBy(F.col("avg_budget").desc_nulls_last())
)
