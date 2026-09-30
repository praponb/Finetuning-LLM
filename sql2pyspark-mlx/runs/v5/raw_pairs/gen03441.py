from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

ranked = (
    df_departments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("budget") >= 3357000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.min("budget").alias("min_budget"),
        F.percentile_approx("budget", 0.95).alias("budget_p95"),
        F.countDistinct("location").alias("distinct_locations"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("min_budget") > 0)
    .orderBy(F.col("min_budget").desc())
)
