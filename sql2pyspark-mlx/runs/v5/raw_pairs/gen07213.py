from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("discount").desc(), "id")
w2 = Window.partitionBy("channel")
w3 = Window.orderBy(F.col("discount").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-05-23"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "discount",
        "shipping_fee",
        F.when(F.col("shipping_fee") >= 35, "High").when(F.col("shipping_fee") >= 7, "Medium").otherwise("Low").alias("shipping_fee_level"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "shipping_fee",
        F.ntile(4).over(w).alias("discount_bucket"),
        F.corr("discount", "shipping_fee").over(w2).alias("corr_discount_shipping_fee"),
        F.rank().over(w3).alias("discount_rank"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.status").alias("payment_status"),
        F.col("c.amount").alias("payment_amount"),
    )
)

cleaned = (
    latest_match
    .filter(F.col("customer_id").isNotNull())
    .withColumn("discount_bucket_num", F.col("discount_bucket").cast("double"))
)

latest_match_orders = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("order_item_quantity"))
)

eligible = (
    latest_match_orders
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.order_id").isin([8, 25, 27])), "left_anti")
)

matched = (
    eligible
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.order_date") >= F.col("pr.starts_on")) & (F.col("s.order_date") <= F.date_add(F.col("pr.ends_on"), 7)), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "shipping_fee",
        "discount_bucket",
        "payment_status",
        "payment_amount",
        "discount_bucket_num",
    )
    .orderBy(F.col("customer_id").desc_nulls_last())
)
