from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w2 = Window.orderBy("region_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region", "city").orderBy(F.col("region_length").desc(), "id")
w_part = Window.orderBy("region_length_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("sqft", "id")
w_running = Window.orderBy("running_max_region_length_bucket", "id").rowsBetween(-3, -1)
w7 = Window.orderBy("running_count_region_length_bucket", "id")
w8 = Window.orderBy("running_max_region_length_bucket", "id")

base = (
    df_stores
    .filter(F.col("region").isNotNull())
    .withColumn("region_length", F.length("region"))
    .withColumn("region_group", F.when(F.col("region").isin(["North", "Central"]), "Core").otherwise("Other"))
)

metrics = (
    base
    .select(
        "id",
        "store_name",
        "sqft",
        "region_length",
        F.ntile(4).over(w).alias("sqft_bucket"),
        F.sum("region_length").over(w2).alias("running_region_length"),
        F.ntile(3).over(w3).alias("region_length_bucket"),
    )
)

metrics_stores = (
    metrics
    .select(
        "id",
        "sqft",
        "sqft_bucket",
        "running_region_length",
        "region_length_bucket",
        F.min("region_length_bucket").over(w_part).alias("running_min_region_length_bucket"),
        F.max("region_length_bucket").over(w_part).alias("running_max_region_length_bucket"),
        F.count("region_length_bucket").over(w_part).alias("running_count_region_length_bucket"),
        F.lag("running_region_length", 1).over(w5).alias("prev_running_region_length"),
    )
)

enriched = (
    metrics_stores
    .select(
        "*",
        F.avg("region_length_bucket").over(w_running).alias("region_length_bucket_avg_3_prior"),
        F.lead("sqft_bucket", 2, 0).over(w7).alias("next_sqft_bucket"),
        F.lead("running_min_region_length_bucket", 1).over(w8).alias("next_running_min_region_length_bucket"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.revenue") > 2900), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "sqft",
        "sqft_bucket",
        "running_region_length",
        "region_length_bucket",
        "running_min_region_length_bucket",
        "region_length_bucket_avg_3_prior",
        "next_sqft_bucket",
        "next_running_min_region_length_bucket",
    )
    .orderBy(F.col("region_length_bucket").asc_nulls_first(), "id")
)
