from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("shipping_fee", "id")
w2 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("store_id", "channel")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

with_windows = (
    df_orders
    .select(
        "*",
        F.ntile(10).over(w).alias("shipping_fee_bucket"),
        F.first("discount", ignorenulls=True).over(w2).alias("first_known_discount"),
    )
)

with_window = (
    with_windows
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.order_date") >= F.col("sub.start_date")) & (F.col("src.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_window_orders = (
    with_window
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.order_date") >= F.col("sub.start_date")) & (F.col("a.order_date") <= F.date_add(F.col("sub.end_date"), 3)), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

matched_keys = (
    with_window_orders
    .select("id", "store_id")
    .subtract(
        df_orders
        .filter(F.col("channel") == "partner")
        .select("id", "store_id")
    )
)

verified = (
    with_window_orders
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.age"), F.col("d.name"))
)

scored = (
    enriched
    .withColumn("corr_discount_amount", F.corr("discount", "amount").over(w3))
    .withColumn("corr_discount_amount_2", F.corr("discount", "amount").over(w4))
)

pivoted = (
    scored
    .groupBy("store_id")
    .pivot("status", ["PENDING", "CANCELLED"])
    .agg(F.avg("subscription_id").alias("amount"))
    .select(
        "store_id",
        F.col("PENDING").alias("pending"),
        F.col("CANCELLED").alias("cancelled"),
    )
)

result = (
    pivoted
    .join(quantity_by_store_id, on="store_id", how="left")
    .select("store_id", "pending", "cancelled", F.col("sum_inventory_quantity"))
    .orderBy(F.col("store_id").desc_nulls_last(), "cancelled")
)
