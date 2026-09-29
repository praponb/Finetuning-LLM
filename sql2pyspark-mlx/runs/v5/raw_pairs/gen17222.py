from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("city", "store_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("running_max_sqft", "id")
discount_by_store_id = df_orders.groupBy("store_id").agg(F.sum("discount").alias("sum_order_discount"))

base = (
    df_inventory
    .filter(F.col("quantity").isNotNull())
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

joined = (
    base
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.quantity",
        "src.last_restock",
        "src.quantity_rounded",
        F.col("b.store_name"),
        F.col("b.sqft"),
    )
)

joined_inventory = (
    joined
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.quantity_rounded",
        "s.sqft",
        F.col("p2.store_name").alias("store_store_name"),
        F.col("p2.city"),
        F.col("p2.opened_date"),
    )
)

with_windows = (
    joined_inventory
    .select(
        "*",
        F.min("sqft").over(w_part).alias("running_min_sqft"),
        F.max("sqft").over(w_part).alias("running_max_sqft"),
        F.sum("sqft").over(w_part).alias("running_sum_sqft"),
        F.avg("sqft").over(w_part).alias("running_avg_sqft"),
    )
)

with_windows_inventory = (
    with_windows
    .withColumn("prev_running_min_sqft", F.lag("running_min_sqft").over(w2))
    .withColumn("prev_running_avg_sqft", F.lag("running_avg_sqft").over(w2))
    .withColumn("next_running_avg_sqft", F.lead("running_avg_sqft").over(w2))
    .withColumn("seq_no", F.row_number().over(w2))
)

result = (
    with_windows_inventory
    .join(discount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "quantity_rounded",
        "sqft",
        "store_store_name",
        "city",
        "running_max_sqft",
        "running_avg_sqft",
        "prev_running_min_sqft",
        F.col("sum_order_discount"),
    )
    .orderBy("id")
)
