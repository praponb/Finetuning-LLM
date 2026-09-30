from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-5, -1)
w2 = Window.orderBy(F.col("sqft").desc(), "id")

eligible = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.opened_date")), "left_anti")
)

cleaned = (
    eligible
    .filter(F.col("opened_date") >= F.lit("2023-07-10").cast("date"))
    .withColumn("city_upper", F.upper("city"))
    .withColumn("resolved_region", F.coalesce("region", F.lit("DIRECT")))
)

metrics = (
    cleaned
    .withColumn("sqft_avg_5_prior", F.avg("sqft").over(w))
    .withColumn("sqft_bucket", F.ntile(4).over(w2))
)

kept = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.units").between(25, 27)), "left_semi")
)

daily = (
    kept
    .rollup(
        F.when(F.col("sqft_bucket") >= 8, "HIGH").otherwise("LOW").alias("sqft_bucket_tier"),
        F.when(F.col("sqft_avg_5_prior") >= 16164, "LARGE").otherwise("SMALL").alias("sqft_avg_5_prior_tier"),
    )
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_count"),
    )
    .filter(F.col("distinct_ids") < 1)
)

result = (
    daily
    .select(
        "sqft_bucket_tier",
        "sqft_avg_5_prior_tier",
        "num_regions",
        "distinct_ids",
        "north_count",
    )
    .orderBy(F.col("sqft_bucket_tier").desc())
)
