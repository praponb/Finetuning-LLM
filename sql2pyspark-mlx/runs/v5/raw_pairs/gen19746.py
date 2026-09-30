from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")

latest = (
    df_store_sales
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 4200, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "sale_date"))
    .withColumn("week_start", F.date_trunc("WEEK", "sale_date"))
    .rollup("revenue_tier", "activity_month", "week_start")
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.percentile_approx("revenue", 0.95).alias("revenue_p95"),
        F.grouping("revenue_tier").alias("grouping_level"),
    )
    .orderBy(F.col("distinct_store_ids").desc())
)
