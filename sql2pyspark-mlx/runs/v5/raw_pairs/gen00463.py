from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("region").orderBy(F.col("store_sale_units").desc_nulls_last(), "id")

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.concat_ws(",", "region", "city").alias("region_city_key"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_clean"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock").alias("inventory_last_restock"))
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.opened_date",
        "s.sqft",
        "s.inventory_last_restock",
        F.col("c.revenue").alias("store_sale_revenue"),
        F.col("c.units").alias("store_sale_units"),
    )
)

deduped = (
    with_latest
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "region", "sqft", "store_sale_revenue", "store_sale_units")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("store_sale_revenue") >= 3200, "HIGH").otherwise("LOW").alias("store_sale_revenue_tier"),
    )
    .agg(
        F.count(F.when(F.col("region") == "South", 1)).alias("south_rows"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.sum(F.when(F.col("region") == "West", F.col("store_sale_units")).otherwise(0)).alias("west_store_sale_units"),
    )
    .filter(F.col("east_count") < 1000)
    .orderBy("south_rows")
)
