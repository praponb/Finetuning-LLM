from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_last(), "id")
w2 = Window.partitionBy("revenue_tier").orderBy(F.col("revenue_p95").desc())

latest = (
    df_store_sales
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "store_id", "sale_date", "units", "revenue")
)

result = (
    latest
    .groupBy(
        F.when(F.col("revenue") >= 3700, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
        "store_id",
    )
    .agg(
        F.corr("revenue", "units").alias("corr_revenue_units"),
        F.min("revenue").alias("min_revenue"),
        F.percentile_approx("revenue", 0.95).alias("revenue_p95"),
    )
    .withColumn("revenue_p95_percent_rank", F.percent_rank().over(w2))
    .orderBy(F.col("min_revenue").desc_nulls_first())
)
