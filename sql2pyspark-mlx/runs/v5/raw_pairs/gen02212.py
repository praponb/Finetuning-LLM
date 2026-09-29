from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("region").orderBy("store_sale_units", "id")
w4 = Window.partitionBy("region")

valid = (
    df_stores
    .filter(F.col("region") == "Central")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.product_id") == 6), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.amount").between(760, 860)), "left_anti")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.region",
        "s.sqft",
        "s.region_filled",
        "s.store_name_normalized",
        F.col("c.units").alias("store_sale_units"),
    )
)

with_latest_stores = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.last_restock"))
)

with_windows = (
    with_latest_stores
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("store_sale_units_share", F.round((F.col("store_sale_units") / F.sum("store_sale_units").over(w4)), 4))
    .withColumn("next_sqft", F.lead("sqft", 2).over(w3))
)

result = (
    with_windows
    .select(
        "id",
        "store_name",
        "region",
        "region_filled",
        "last_restock",
        "event_seq",
        "store_sale_units_share",
    )
    .orderBy(F.col("last_restock").desc())
)
