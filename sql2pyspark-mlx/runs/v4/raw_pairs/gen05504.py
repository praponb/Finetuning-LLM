from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("region", "city").orderBy("sqft", "id")
w3 = Window.orderBy("opened_date", "id")
w_part = Window.partitionBy("city").orderBy("inventory_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("sqft", "id")
w_recent = Window.partitionBy("city").orderBy("sqft", "id")

base = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2024-02-01").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
        F.upper(F.trim("store_name")).alias("store_name_normalized_2"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock").alias("inventory_last_restock"))
)

enriched = (
    latest_match
    .withColumn("prev_sqft", F.lag("sqft", 1).over(w_hist))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w_part))
    .withColumn("sqft_bucket", F.ntile(3).over(w5))
    .withColumn("sqft_bucket_2", F.ntile(3).over(w_recent))
)

result = (
    enriched
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        "store_name_normalized",
        "store_name_normalized_2",
        "inventory_last_restock",
        "event_seq",
    )
    .orderBy(F.col("opened_date").desc_nulls_last())
)
