from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("revenue").desc())
w2 = Window.partitionBy("product_id", "store_id")
w3 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("product_id").orderBy(F.col("revenue").asc_nulls_first())
w5 = Window.partitionBy("store_id").orderBy("units", "id")

scored = (
    df_store_sales
    .withColumn("revenue_rank", F.rank().over(w))
    .withColumn("corr_units_revenue", F.corr("units", "revenue").over(w2))
    .withColumn("units_avg_6_prior", F.avg("units").over(w3))
    .withColumn("revenue_rank_2", F.rank().over(w4))
    .withColumn("units_bucket", F.ntile(4).over(w5))
)

result = (
    scored
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 3600, "PREMIUM").otherwise("STANDARD"))
    .withColumn("week_start", F.date_trunc("WEEK", "sale_date"))
    .rollup("revenue_tier", "week_start", "store_id")
    .agg(
        F.percentile_approx("corr_units_revenue", [0.25, 0.5, 0.75]).alias("corr_units_revenue_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("revenue_tier").alias("grouping_id"),
    )
    .filter(F.col("distinct_ids") > 1)
)
