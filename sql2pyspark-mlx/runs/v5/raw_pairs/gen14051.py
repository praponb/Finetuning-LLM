from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id")
w3 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("sqft_bucket", "id")
w5 = Window.partitionBy("city").orderBy(F.col("opened_date").asc_nulls_last())
w6 = Window.orderBy(F.col("next_sqft").desc(), "id")
w7 = Window.orderBy(F.col("sqft_bucket_times_sqft_bucket_bucket").desc(), "id")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

metrics = (
    df_stores
    .select(
        "*",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.ntile(4).over(w2).alias("sqft_bucket"),
        F.sum("sqft").over(w3).alias("running_sqft_2"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.lead("sqft", 2).over(w4).alias("next_sqft"),
        F.ntile(3).over(w4).alias("sqft_bucket_bucket"),
    )
)

ranked = (
    scored
    .withColumn("rank_in_group", F.rank().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

valid = (
    ranked
    .filter(F.col("next_sqft").between(4695, 12145))
    .select(
        "*",
        (F.col("sqft_bucket") * F.col("sqft_bucket_bucket")).alias("sqft_bucket_times_sqft_bucket_bucket"),
        F.coalesce("region", F.lit("OTHER")).alias("region_clean"),
    )
)

metrics_stores = (
    valid
    .select(
        "*",
        F.row_number().over(w6).alias("seq_no"),
        F.ntile(3).over(w7).alias("sqft_bucket_times_sqft_bucket_bucket_bucket"),
    )
)

cleaned = (
    metrics_stores
    .filter((F.col("opened_date") >= F.lit("2023-06-06").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        "running_sqft",
        "sqft_bucket",
        "next_sqft",
        "sqft_bucket_bucket",
        "sqft_bucket_times_sqft_bucket_bucket",
        "seq_no",
        "sqft_bucket_times_sqft_bucket_bucket_bucket",
        F.length("city").alias("city_length"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "opened_date",
        "sqft",
        "running_sqft",
        "sqft_bucket",
        "sqft_bucket_bucket",
        "sqft_bucket_times_sqft_bucket_bucket",
        "seq_no",
        "sqft_bucket_times_sqft_bucket_bucket_bucket",
    )
)
