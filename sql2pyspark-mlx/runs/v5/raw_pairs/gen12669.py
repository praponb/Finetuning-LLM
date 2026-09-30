from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id")
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("status").orderBy("payment_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("status_min_shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy("discount_per_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.min("revenue").alias("min_store_sale_revenue"))

filtered = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.col("shipping_fee") < 16))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.round((F.col("discount") / F.col("amount")), 2).alias("discount_per_amount"),
    )
)

with_windows = (
    filtered
    .filter(F.col("shipping_fee") > 37)
    .select(
        "*",
        F.min("shipping_fee").over(w).alias("status_min_shipping_fee"),
        F.corr("shipping_fee", "amount").over(w2).alias("corr_shipping_fee_amount"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.method").alias("payment_method"),
        F.col("c.amount").alias("payment_amount"),
    )
)

with_windows_orders = (
    with_last
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "shipping_fee",
        "discount_per_amount",
        "status_min_shipping_fee",
        "corr_shipping_fee_amount",
        F.sum("amount").over(w4).alias("cumulative_amount"),
        F.sum("corr_shipping_fee_amount").over(w5).alias("cumulative_corr_shipping_fee_amount"),
        F.first("discount_per_amount", ignorenulls=True).over(w6).alias("first_known_discount_per_amount"),
    )
)

totals = (
    with_windows_orders
    .groupBy("store_id", F.date_trunc("YEAR", "order_date").alias("year_start"))
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    totals
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "year_start",
        "distinct_status",
        "num_rows",
        F.col("min_store_sale_revenue"),
    )
    .orderBy(F.col("year_start").asc_nulls_last())
)
