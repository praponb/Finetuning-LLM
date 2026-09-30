from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())
w2 = Window.orderBy("unit_price", "id")
w3 = Window.orderBy(F.col("quantity").desc(), "id")
w4 = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price").desc(), "id")
w_recent = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc_nulls_first())
w6 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w7 = Window.partitionBy("product_id")
w8 = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id")

enriched = (
    df_order_items
    .select(
        "*",
        F.cume_dist().over(w).alias("quantity_cume_dist"),
        F.ntile(5).over(w2).alias("unit_price_bucket"),
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w4).alias("row_seq_2"),
        F.rank().over(w_recent).alias("unit_price_rank"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.amount"))
)

valid = (
    joined
    .filter(F.col("product_id").isNotNull())
    .withColumn("unit_price_bucket_plus_row_seq_2", (F.col("unit_price_bucket") + F.col("row_seq_2")))
    .withColumn("unit_price_plus_unit_price_bucket", (F.col("unit_price") + F.col("unit_price_bucket")))
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .filter(F.col("dedup_rank") > 1)
    .select(
        "id",
        "product_id",
        "quantity",
        "row_seq",
        "unit_price_rank",
        "amount",
        "unit_price_plus_unit_price_bucket",
        "dedup_rank",
        F.round((F.col("unit_price_rank") / F.sum("unit_price_rank").over(w7)), 4).alias("unit_price_rank_share"),
        F.ntile(4).over(w8).alias("quantity_bucket"),
    )
)
