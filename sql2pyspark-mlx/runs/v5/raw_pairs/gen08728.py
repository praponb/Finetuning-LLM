from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price")
w2 = Window.partitionBy("product_id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.avg("amount").alias("avg_payment_amount"))

filtered = (
    df_order_items
    .filter(F.col("unit_price") >= 130)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)

matched_keys = (
    filtered
    .select("product_id", "order_id")
    .intersectAll(
        df_order_items
        .filter(F.col("order_id") == 11)
        .select("product_id", "order_id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .filter(F.col("product_id").isin([4, 9, 32]))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.percent_rank().over(w).alias("unit_price_percent_rank"),
        F.stddev_samp("quantity").over(w2).alias("product_id_stddev_samp_quantity"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.shipping_fee"), F.col("b.status"))
)

cleaned = (
    with_dims
    .filter(F.col("status").eqNullSafe(F.lit("RETURNED")))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "product_id_stddev_samp_quantity",
        "shipping_fee",
        "status",
        F.trim("status").alias("clean_status"),
    )
)

result = (
    cleaned
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "product_id_stddev_samp_quantity",
        "shipping_fee",
        "status",
        "clean_status",
        F.col("avg_payment_amount"),
    )
)
