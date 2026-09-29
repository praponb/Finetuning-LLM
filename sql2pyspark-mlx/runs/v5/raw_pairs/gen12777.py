from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_first(), "id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("product_id", "order_id")
w4 = Window.orderBy(F.col("amount").desc_nulls_first())
w5 = Window.partitionBy("order_id").orderBy(F.col("amount").desc())
w6 = Window.orderBy(F.col("unit_price_share").desc_nulls_first())
w7 = Window.partitionBy("product_id")
w_ordered_2 = Window.orderBy(F.col("amount_percent_rank").desc_nulls_last())

latest = (
    df_order_items
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_dims = (
    latest
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.order_date"))
)

with_dims_order_items = (
    with_dims
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.order_date").alias("order_order_date"), F.col("o.amount"))
)

remaining_keys = (
    with_dims_order_items
    .select("id")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isin([15, 38]))
        .select("id")
    )
)

reconciled = (
    with_dims_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .filter(F.col("order_date") <= "2024-02-01")
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4).alias("unit_price_share"),
        F.count("quantity").over(w2).alias("order_id_count_quantity"),
        F.avg("quantity").over(w3).alias("product_id_avg_quantity"),
        F.percent_rank().over(w4).alias("amount_percent_rank"),
        F.rank().over(w5).alias("amount_rank"),
    )
)

enriched = (
    scored
    .withColumn("unit_price_share_dense_rank", F.dense_rank().over(w6))
    .withColumn("product_id_min_product_id_avg_quantity", F.min("product_id_avg_quantity").over(w7))
)

scored_order_items = (
    enriched
    .select(
        "order_id",
        "product_id",
        "unit_price",
        "unit_price_share",
        "order_id_count_quantity",
        "product_id_avg_quantity",
        "unit_price_share_dense_rank",
        F.stddev_samp("unit_price_share_dense_rank").over(w7).alias("product_id_stddev_samp_unit_price_share_dense_rank"),
        F.dense_rank().over(w_ordered_2).alias("amount_percent_rank_dense_rank"),
    )
)

result = (
    scored_order_items
    .select(
        "order_id",
        "product_id",
        "unit_price_share",
        "product_id_avg_quantity",
        "unit_price_share_dense_rank",
        "amount_percent_rank_dense_rank",
    )
    .orderBy("order_id")
)
