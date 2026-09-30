from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("is_new_york", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w_running = Window.orderBy("sqft", "id")

valid = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-07-01").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.concat("region", F.lit(" "), "city").alias("region_city"),
    )
)

cleaned = (
    valid
    .filter(F.col("city") != "Singapore")
    .select(
        "id",
        "city",
        "sqft",
        "region_city",
        F.when(F.col("city") == "New York", 1).otherwise(0).alias("is_new_york"),
    )
)

with_windows = (
    cleaned
    .withColumn("sqft_avg_6", F.avg("sqft").over(w))
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w2))
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.sqft",
        "s.is_new_york",
        "s.sqft_avg_6",
        "s.first_known_sqft",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") <= F.col("s.inventory_last_restock")), "left_semi")
)

with_windows_stores = (
    not_flagged
    .select(
        "id",
        "sqft",
        "inventory_last_restock",
        "quantity",
        F.lag("sqft").over(w_running).alias("prev_sqft"),
        F.lead("sqft").over(w_running).alias("next_sqft"),
        F.lag("first_known_sqft").over(w_running).alias("prev_first_known_sqft"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

eligible = (
    with_windows_stores
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id") == 18), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "sqft",
        "inventory_last_restock",
        "quantity",
        "prev_sqft",
        "prev_first_known_sqft",
    )
)
