from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("order_id").orderBy(F.col("unit_price_filled").desc_nulls_last())
w3 = Window.partitionBy("status")

cleaned = (
    df_order_items
    .filter(F.col("quantity") != 4)
    .withColumn("unit_price_filled", F.coalesce("unit_price", F.lit(0)))
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.status"))
)

remaining_keys = (
    with_dims
    .select("order_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") >= 4)
        .select("order_id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.stddev_samp("unit_price_filled").over(w).alias("order_id_stddev_samp_unit_price_filled"),
        F.rank().over(w2).alias("unit_price_filled_rank"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share"),
    )
)

result = (
    scored
    .select(
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "unit_price_filled",
        "status",
        "order_id_stddev_samp_unit_price_filled",
        "unit_price_filled_rank",
        F.when(F.col("status").rlike("^(PENDING|CANCELLED)$"), "OTHER_ZONE").when(F.col("status").rlike("^(COMPLETED|SHIPPED)$"), "CORE").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("status_zone").desc_nulls_last())
)
