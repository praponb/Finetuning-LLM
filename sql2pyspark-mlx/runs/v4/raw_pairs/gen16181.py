from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("budget").asc_nulls_last())
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

metrics = (
    df_departments
    .withColumn("last_known_location", F.last("location", ignorenulls=True).over(w))
    .withColumn("budget_rank", F.rank().over(w2))
    .withColumn("budget_bucket", F.ntile(10).over(w3))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("budget_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("budget_rank_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_count"),
        F.percentile_approx("budget_bucket", [0.25, 0.5, 0.75]).alias("budget_bucket_quartiles"),
        F.min("budget").alias("min_budget"),
        F.count("*").alias("row_count_2"),
        F.min("budget_rank").alias("min_budget_rank"),
    )
)
