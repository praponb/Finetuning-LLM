from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("carrier", "order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w4 = Window.partitionBy("status").orderBy("running_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy(F.col("running_cost_2").asc_nulls_first())
w7 = Window.partitionBy("order_id")
w8 = Window.orderBy(F.col("running_cost_2").asc_nulls_last())
w9 = Window.partitionBy("status")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.min("amount").alias("min_payment_amount"))

filtered = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-09-02").cast("date")) & F.col("order_id").isNotNull() & F.col("cost").between(51, 62))
    .withColumn("carrier_status", F.concat("carrier", F.lit(", "), "status"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("UNKNOWN")))
)

scored = (
    filtered
    .select(
        "*",
        F.sum("cost").over(w).alias("running_cost"),
        F.sum("cost").over(w_hist).alias("running_cost_2"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

with_windows = (
    scored
    .withColumn("first_known_seq_no", F.first("seq_no", ignorenulls=True).over(w4))
    .withColumn("last_known_running_cost_2", F.last("running_cost_2", ignorenulls=True).over(w5))
)

kept_keys = (
    with_windows
    .select("status", "id")
    .exceptAll(
        df_shipments
        .filter(F.year("delivered_date") == 2025)
        .select("status", "id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.shipping_fee"), F.col("b.amount"))
)

scored_shipments = (
    with_dims
    .withColumn("running_cost_2_percent_rank", F.percent_rank().over(w6))
    .withColumn("running_cost_2_share", F.round((F.col("running_cost_2") / F.sum("running_cost_2").over(w7)), 4))
    .withColumn("running_cost_2_cume_dist", F.cume_dist().over(w8))
    .withColumn("first_known_seq_no_share", F.round((F.col("first_known_seq_no") / F.sum("first_known_seq_no").over(w9)), 4))
)

result = (
    scored_shipments
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "carrier_filled",
        "running_cost",
        F.when(F.col("carrier").rlike("^(DHL|UPS)$"), "GROUP_B").when(F.col("carrier").rlike("^(FedEx|Kerry)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("carrier_zone"),
        F.col("min_payment_amount"),
    )
    .orderBy(F.col("status").desc())
)
