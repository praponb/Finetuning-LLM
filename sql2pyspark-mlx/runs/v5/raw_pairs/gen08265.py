from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_first())
w_hist = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("product_id", "order_id")
w_recent = Window.partitionBy("product_id")
w5 = Window.partitionBy("product_id").orderBy("unit_price_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc())
w7 = Window.partitionBy("status").orderBy(F.col("unit_price").asc_nulls_first(), "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.min("amount").alias("min_payment_amount"))

with_windows = (
    df_order_items
    .filter(F.col("quantity").between(4, 6))
    .select(
        "*",
        F.rank().over(w).alias("quantity_rank"),
        F.avg("quantity").over(w_hist).alias("quantity_avg_5"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity_rank",
        F.sum("quantity_rank").over(w_recent).alias("product_id_sum_quantity_rank"),
        F.sum("unit_price").over(w5).alias("running_unit_price"),
        F.percent_rank().over(w6).alias("quantity_percent_rank"),
        F.corr("unit_price", "quantity").over(w_recent).alias("corr_unit_price_quantity"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .select("e.*", F.col("o.status"))
)

ranked = (
    with_dims
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

with_dims_order_items = (
    ranked
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.unit_price",
        "a.quantity_rank",
        "a.product_id_sum_quantity_rank",
        "a.running_unit_price",
        F.col("d.shipping_fee"),
        F.col("d.status").alias("order_status"),
    )
)

filtered = (
    with_dims_order_items
    .filter(F.col("order_status").isNotNull())
    .withColumn("shipping_fee_band", F.when(F.col("shipping_fee") >= 37, "High").when(F.col("shipping_fee") >= 17, "Medium").otherwise("Low"))
    .withColumn("resolved_order_status", F.coalesce("order_status", F.lit("DIRECT")))
)

kept_keys = (
    filtered
    .select("order_id", "id")
    .intersect(
        df_order_items
        .filter(F.col("unit_price") >= 200)
        .select("order_id", "id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "running_unit_price",
        "shipping_fee",
        "order_status",
        "shipping_fee_band",
        F.when(F.col("order_status").rlike("^(RETURNED|CANCELLED)$"), "EXTENDED").when(F.col("order_status").rlike("^(COMPLETED|PENDING)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("order_status_zone"),
        F.col("min_payment_amount"),
    )
)
