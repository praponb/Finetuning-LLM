from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("region_length").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("region_length").desc(), "id")
w3 = Window.partitionBy("region", "city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("region_length", "id")
w5 = Window.partitionBy("region").orderBy("region_length_bucket", "id")
w6 = Window.partitionBy("region").orderBy(F.col("running_region_length").desc(), "id")
w7 = Window.partitionBy("region", "city").orderBy(F.col("region_length_bucket_2").desc_nulls_last())

kept = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.quantity") != 203), "left_semi")
)

filtered = (
    kept
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.length("region").alias("region_length"),
        F.upper("region").alias("region_upper"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.ntile(10).over(w).alias("region_length_bucket"),
        F.ntile(4).over(w2).alias("region_length_bucket_2"),
        F.sum("region_length").over(w3).alias("running_region_length"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.lag("sqft", 2, 0).over(w4).alias("prev_sqft"),
        F.lag("region_length_bucket_2", 1).over(w5).alias("prev_region_length_bucket_2"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

latest = (
    ranked
    .withColumn("latest_rank", F.rank().over(w7))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

base = (
    latest
    .filter(F.col("prev_region_length_bucket_2") != 4)
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        "region_length",
        "region_length_bucket",
        "region_length_bucket_2",
        "prev_sqft",
        "prev_region_length_bucket_2",
        "dedup_rank",
        F.when(F.col("city").isin(["New York", "London"]), "Priority").otherwise("Other").alias("city_group"),
    )
)

result = (
    base
    .groupBy("city")
    .pivot("region", ["South", "Central"])
    .agg(F.avg("region_length_bucket_2").alias("total"))
    .select("city", F.col("South").alias("south"), F.col("Central").alias("central"))
)
