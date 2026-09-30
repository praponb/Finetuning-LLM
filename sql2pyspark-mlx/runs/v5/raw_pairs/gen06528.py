from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.min("units").alias("min_store_sale_units"))

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-05-16").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.amount").alias("payment_amount"),
        F.col("c.status").alias("payment_status"),
    )
)

remaining_keys = (
    latest_match
    .select("status", "channel")
    .subtract(
        df_orders
        .filter(F.col("status").isin(["RETURNED", "CANCELLED", "SHIPPED"]))
        .select("status", "channel")
    )
)

reconciled = (
    latest_match
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

valid_orders = (
    reconciled
    .filter((F.col("order_date") > F.lit("2023-06-15").cast("date")) & F.col("channel").isin(["mobile", "web"]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "discount_filled",
        "payment_amount",
        "payment_status",
        F.lower("status").alias("status_lower"),
    )
)

not_flagged = (
    valid_orders
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.amount") <= 1350), "left_anti")
)

result = (
    not_flagged
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "customer_id",
        "store_id",
        "order_date",
        "shipping_fee",
        "payment_amount",
        "payment_status",
        "status_lower",
        F.when(F.col("payment_status").rlike("^(SUCCESS|FAILED)$"), "GROUP_A").when(F.col("payment_status").rlike("^(REFUNDED)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("payment_status_zone"),
        F.col("min_store_sale_units"),
    )
)
