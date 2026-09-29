from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("region").orderBy("store_sale_revenue", "id")
w3 = Window.partitionBy("city").orderBy("store_sale_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .withColumn("store_name_length", F.length("store_name"))
    .withColumn("resolved_region", F.coalesce("region", F.lit("UNKNOWN")))
)

latest_match = (
    valid
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.region",
        "s.opened_date",
        "s.sqft",
        "s.store_name_length",
        F.col("c.units"),
        F.col("c.sale_date"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.sale_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.units").alias("store_sale_units"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

valid_stores = (
    with_latest
    .filter((F.col("opened_date") > F.lit("2023-07-19").cast("date")) & F.col("region").isNotNull())
    .withColumn("store_sale_units_times_sqft", (F.col("store_sale_units") * F.col("sqft")))
    .withColumn("region_filled", F.coalesce("region", F.lit("N/A")))
)

metrics = (
    valid_stores
    .filter(F.col("city") == "Berlin")
    .select(
        "id",
        "sqft",
        "store_name_length",
        "units",
        "store_sale_units",
        "store_sale_revenue",
        "store_sale_units_times_sqft",
        "region_filled",
        F.lead("store_sale_units_times_sqft").over(w_recent).alias("next_store_sale_units_times_sqft"),
        F.lead("units").over(w_recent).alias("next_units"),
        F.lag("units").over(w_recent).alias("prev_units"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.last("store_sale_units_times_sqft", ignorenulls=True).over(w3).alias("last_known_store_sale_units_times_sqft"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.channel").isin(["web", "store"])), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.shipping_fee") <= 19), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("units") >= 37, "HIGH").otherwise("LOW").alias("units_tier"),
        F.when(F.col("store_sale_units") >= 41, "HIGH").otherwise("LOW").alias("store_sale_units_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("store_name_length").alias("avg_store_name_length"),
        F.max("store_sale_units_times_sqft").alias("max_store_sale_units_times_sqft"),
    )
    .orderBy(F.col("units_tier").desc_nulls_last())
)
