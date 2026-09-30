from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock_quarter").desc_nulls_first())
w2 = Window.partitionBy("product_id").orderBy("last_restock_quarter", "id")
w3 = Window.partitionBy("store_id").orderBy("last_restock_quarter", "id")
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

filtered = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-04-19").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.quarter("last_restock").alias("last_restock_quarter"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

metrics = (
    ranked
    .filter(F.year("last_restock") >= 2025)
    .select(
        "id",
        "product_id",
        "last_restock_quarter",
        F.lag("last_restock_quarter").over(w3).alias("prev_last_restock_quarter"),
        F.lead("last_restock_quarter").over(w3).alias("next_last_restock_quarter"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

result = (
    metrics
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "last_restock_quarter",
        "prev_last_restock_quarter",
        "next_last_restock_quarter",
        "event_seq",
        F.col("max_order_item_unit_price"),
    )
    .orderBy(F.col("prev_last_restock_quarter").desc())
)
