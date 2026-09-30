from pyspark.sql import functions as F

amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.dayofmonth("delivered_date").alias("delivered_date_day"),
        F.initcap(F.trim("carrier")).alias("carrier_title"),
    )
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.status").alias("order_status"),
        F.col("b.amount"),
        F.col("b.shipping_fee"),
    )
)

valid = (
    with_dims
    .filter(F.col("order_status").isNotNull() & F.col("delivered_date").between("2023-06-13", "2023-12-05"))
    .select(
        "*",
        F.concat("carrier", F.lit(", "), "status").alias("carrier_status"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

daily = (
    valid
    .withColumn("quarter_start", F.date_trunc("QUARTER", "shipped_date"))
    .groupingSets(
        [["order_id", "quarter_start"], ["order_id"], []],
        "order_id",
        "quarter_start",
    )
    .agg(
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.stddev_samp("cost").alias("cost_stddev"),
    )
)

result = (
    daily
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "quarter_start",
        "num_carriers",
        "carrier_list",
        "cost_stddev",
        F.col("sum_payment_amount"),
    )
)
