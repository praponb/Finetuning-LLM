from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("quantity_filled").asc_nulls_first())
w3 = Window.orderBy("quantity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("quantity_filled", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("product_id", "store_id").orderBy("quantity_filled", "id")
amount_by_store_id = df_orders.groupBy("store_id").agg(F.avg("amount").alias("avg_order_amount"))

valid = (
    df_inventory
    .filter(F.col("quantity").between(108, 207))
    .select(
        "id",
        "store_id",
        "product_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.when(F.col("quantity") >= 219, "High").when(F.col("quantity") >= 116, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

metrics = (
    valid
    .filter(F.col("product_id").isin([2, 4, 9]))
    .withColumn("cumulative_quantity_filled", F.sum("quantity_filled").over(w))
    .withColumn("quantity_filled_cume_dist", F.cume_dist().over(w2))
    .withColumn("cumulative_quantity_filled_2", F.sum("quantity_filled").over(w3))
    .withColumn("quantity_filled_avg_3_prior", F.avg("quantity_filled").over(w4))
    .withColumn("cumulative_quantity_filled_3", F.sum("quantity_filled").over(w3))
)

scored = (
    metrics
    .select(
        "*",
        F.lag("quantity_filled_cume_dist").over(w5).alias("prev_quantity_filled_cume_dist"),
        F.lead("quantity_filled_cume_dist").over(w5).alias("next_quantity_filled_cume_dist"),
        F.lag("cumulative_quantity_filled").over(w5).alias("prev_cumulative_quantity_filled"),
        F.lead("cumulative_quantity_filled").over(w5).alias("next_cumulative_quantity_filled"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

matched_keys = (
    scored
    .select("id", "product_id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") >= "2023-07-01")
        .select("id", "product_id")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(amount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "quantity_tier",
        "cumulative_quantity_filled",
        "quantity_filled_cume_dist",
        "cumulative_quantity_filled_2",
        "quantity_filled_avg_3_prior",
        "prev_quantity_filled_cume_dist",
        "next_quantity_filled_cume_dist",
        F.col("avg_order_amount"),
    )
    .orderBy(F.col("next_quantity_filled_cume_dist").desc_nulls_last())
)
