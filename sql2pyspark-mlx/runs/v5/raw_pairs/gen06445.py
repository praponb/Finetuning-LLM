from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock_period", "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").asc_nulls_first(), "id")
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

ranked = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "store_id", "product_id", "quantity", "last_restock")
)

filtered = (
    ranked
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-03-23")))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("last_restock_period").over(w2).isNull() | (F.col("last_restock_period") > F.lag("last_restock_period").over(w2) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "last_restock_period",
        F.sum("is_new_session").over(w3).alias("session_id"),
    )
)

prev_events = (
    islands
    .withColumn("prev_last_restock_period", F.lag("last_restock_period").over(w2))
)

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_last_restock_period").isNull() | (F.col("last_restock_period") > F.col("prev_last_restock_period") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands_inventory = (
    gaps
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock_period",
        F.sum("is_new_session").over(w3).alias("session_no"),
    )
)

deduped = (
    islands_inventory
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select("id", "product_id", "quantity", F.col("max_order_item_unit_price"))
)
