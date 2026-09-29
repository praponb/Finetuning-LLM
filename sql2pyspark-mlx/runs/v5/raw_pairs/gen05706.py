from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w2 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-08-13"))) & F.col("region").isin(["Central", "East", "South"]))
    .select(
        "*",
        F.date_add("opened_date", 30).alias("opened_date_plus_30d"),
        F.initcap(F.trim("region")).alias("region_title"),
    )
)

cleaned = (
    valid
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "opened_date_plus_30d",
        "region_title",
        F.round((F.col("sqft") * 0.85), 2).alias("sqft_scaled"),
        F.substring("store_name", 1, 4).alias("store_name_prefix"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"), F.col("c.discount"))
)

metrics = (
    with_last
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        "opened_date_plus_30d",
        "region_title",
        "sqft_scaled",
        "region_filled",
        F.first("sqft_scaled", ignorenulls=True).over(w2).alias("first_known_sqft_scaled"),
        F.last("sqft", ignorenulls=True).over(w3).alias("last_known_sqft"),
    )
)

result = (
    metrics
    .groupBy(
        F.date_trunc("year", "opened_date_plus_30d").alias("year_start"),
        F.when(F.col("last_known_sqft") >= 14426, "HIGH").otherwise("LOW").alias("last_known_sqft_tier"),
    )
    .agg(
        F.count_if(F.col("region") == "West").alias("west_events"),
        F.round(F.sum("last_known_sqft") / F.nullif(F.count(F.when(F.col("region") == "Central", 1)), F.lit(0)), 2).alias("last_known_sqft_per_central"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
        F.count("*").alias("num_rows"),
        F.sum("first_known_sqft_scaled").alias("total_first_known_sqft_scaled"),
    )
)
