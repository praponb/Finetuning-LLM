from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc())
w2 = Window.orderBy(F.col("shipping_fee").asc_nulls_last())
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.max("quantity").alias("max_inventory_quantity"))

filtered = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.year("order_date") >= 2024))
    .withColumn("status_channel_key", F.concat_ws(",", "status", "channel"))
    .withColumn("order_date_label", F.date_format("order_date", "MMM yyyy"))
)

scored = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w).alias("amount_dense_rank"),
        F.percent_rank().over(w2).alias("shipping_fee_percent_rank"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.sqft"), F.col("d.region"), F.col("d.store_name"))
)

wide = (
    joined
    .groupBy("store_id")
    .pivot("region", ["South", "West"])
    .agg(F.sum("shipping_fee_percent_rank"))
)

result = (
    wide
    .join(quantity_by_store_id, on="store_id", how="left")
    .select("store_id", "West", F.col("max_inventory_quantity"))
    .orderBy(F.col("West").desc())
)
