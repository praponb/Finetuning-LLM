from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")

cleaned = (
    df_store_sales
    .filter(F.col("revenue") >= 1800)
    .withColumn("revenue_whole", F.floor("revenue"))
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined = (
    latest
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.opened_date"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .filter(~F.col("p.region").isin(["West", "North"]))
    .select(
        "src.*",
        F.col("p.opened_date").alias("store_opened_date"),
        F.col("p.city"),
        F.col("p.region"),
    )
)

result = (
    with_dims
    .groupBy(
        "region",
        F.when(F.col("revenue") >= 3000, "HIGH").otherwise("LOW").alias("revenue_tier"),
    )
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.sum(F.when(F.col("city") == "London", F.col("revenue_whole")).otherwise(0)).alias("london_revenue_whole"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.percentile_approx("revenue_whole", 0.95).alias("revenue_whole_p95"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .orderBy("revenue_tier")
)
