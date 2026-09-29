from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_last())
w3 = Window.partitionBy("location")
w4 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("location").orderBy("budget_share_2", "id")
w7 = Window.partitionBy("dept_name").orderBy(F.col("budget_cume_dist").desc_nulls_last())
w8 = Window.partitionBy("location").orderBy("budget_cume_dist", "id")
w_hist = Window.orderBy("budget_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("last_known_budget_avg_5_prior_tier").orderBy(F.col("singapore_count").desc())

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.lower("location").alias("location_lower"),
        F.coalesce("location", F.lit("NONE")).alias("location_clean"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.cume_dist().over(w2).alias("budget_cume_dist"),
        F.round((F.col("budget") / F.sum("budget").over(w3)), 4).alias("budget_share_2"),
        F.avg("budget").over(w4).alias("budget_avg_5_prior"),
        F.avg("budget").over(w5).alias("budget_avg_5"),
    )
)

with_windows = (
    metrics
    .withColumn("budget_share_2_bucket", F.ntile(4).over(w6))
    .withColumn("budget_cume_dist_percent_rank", F.percent_rank().over(w7))
    .withColumn("next_budget_avg_5_prior", F.lead("budget_avg_5_prior", 2).over(w8))
    .withColumn("last_known_budget_avg_5_prior", F.last("budget_avg_5_prior", ignorenulls=True).over(w_hist))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("last_known_budget_avg_5_prior") >= 3698612, "HIGH").otherwise("LOW").alias("last_known_budget_avg_5_prior_tier"),
        "location",
    )
    .agg(
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget_avg_5")).otherwise(0)).alias("sales_budget_avg_5"),
    )
    .withColumn("singapore_count_percent_rank", F.percent_rank().over(w10))
    .orderBy(F.col("location").desc_nulls_first())
)
