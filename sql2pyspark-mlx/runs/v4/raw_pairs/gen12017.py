from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("shipping_fee").desc())
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("status", "customer_id")
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("customer_id", "status")

filtered = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.col("channel") == "mobile"))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.concat_ws("/", "channel", "status").alias("channel_status_key"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

remaining_keys = (
    filtered
    .select("customer_id")
    .subtract(
        df_orders
        .filter(F.col("shipping_fee").isNull())
        .select("customer_id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.cost") <= 65), "left_anti")
)

base = (
    kept
    .filter((F.col("order_date") > F.lit("2024-03-19").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.last_day("order_date").alias("order_date_month_end"))
)

metrics = (
    base
    .withColumn("shipping_fee_cume_dist", F.cume_dist().over(w_running))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
    .withColumn("shipping_fee_cume_dist_2", F.cume_dist().over(w_running))
    .withColumn("status_max_amount", F.max("amount").over(w2))
    .withColumn("status_sum_amount", F.sum("amount").over(w3))
)

scored = (
    metrics
    .select(
        "*",
        F.max("shipping_fee").over(w3).alias("status_max_shipping_fee"),
        F.max("shipping_fee_cume_dist_2").over(w4).alias("customer_id_max_shipping_fee_cume_dist_2"),
        F.max("amount_share").over(w5).alias("customer_id_max_amount_share"),
        F.count("amount_share").over(w4).alias("customer_id_count_amount_share"),
    )
)

kept_orders = (
    scored
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") >= F.col("s.order_date")), "left_anti")
)

wide = (
    kept_orders
    .groupBy("customer_id")
    .pivot("status", ["CANCELLED", "PENDING", "COMPLETED"])
    .agg(F.max("shipping_fee_cume_dist").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "customer_id",
        F.col("CANCELLED_amount").alias("cancelled_amount"),
        F.col("CANCELLED_n").alias("cancelled_n"),
        F.col("PENDING_amount").alias("pending_amount"),
        F.col("PENDING_n").alias("pending_n"),
        F.col("COMPLETED_amount").alias("completed_amount"),
        F.col("COMPLETED_n").alias("completed_n"),
    )
)

result = (
    wide
    .select(
        "customer_id",
        "cancelled_amount",
        "pending_amount",
        "completed_amount",
        "completed_n",
    )
)
