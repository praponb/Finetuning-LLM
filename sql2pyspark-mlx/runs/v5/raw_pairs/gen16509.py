from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date_period", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("region").orderBy("sqft_filled", "id")
w4 = Window.partitionBy("region").orderBy("sqft_filled_minus_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-08-21").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") >= F.col("s.opened_date")), "left_semi")
)

base = (
    not_flagged
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "city",
        "region",
        "sqft",
        "sqft_filled",
        (F.col("sqft_filled") - F.col("sqft")).alias("sqft_filled_minus_sqft"),
        F.date_trunc("year", "opened_date").alias("opened_date_period"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

base_stores = (
    base
    .filter(F.col("sqft_filled").between(31, 45))
    .withColumn("sqft_filled_band", F.when(F.col("sqft_filled") >= 70, "High").when(F.col("sqft_filled") >= 33, "Medium").otherwise("Low"))
)

deduped = (
    base_stores
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_latest = (
    deduped
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.sqft_filled",
        "s.sqft_filled_minus_sqft",
        "s.sqft_filled_band",
        "s.dedup_rank",
        F.col("c.order_date"),
        F.col("c.discount"),
    )
)

metrics = (
    with_latest
    .filter(F.col("region") == "South")
    .select(
        "id",
        "sqft_filled_band",
        "discount",
        F.row_number().over(w3).alias("row_seq"),
        F.last("sqft_filled_minus_sqft", ignorenulls=True).over(w4).alias("last_known_sqft_filled_minus_sqft"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("last_known_sqft_filled_minus_sqft") >= 67, "PREMIUM").otherwise("STANDARD").alias("last_known_sqft_filled_minus_sqft_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("last_known_sqft_filled_minus_sqft").alias("min_last_known_sqft_filled_minus_sqft"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)
