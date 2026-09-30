from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_orders
    .filter(F.col("store_id").isNotNull() & F.col("discount").between(0.15, 0.2))
    .select("*", F.initcap(F.trim("status")).alias("status_title"))
)

with_last = (
    valid
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.delivered_date"),
        F.col("c.carrier").alias("shipment_carrier"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") < "2024-08-15"), "left_anti")
)

scored = (
    kept
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "amount",
        "discount",
        "status_title",
        "shipment_carrier",
        F.sum("discount").over(w_recent).alias("last4_sum_discount"),
        F.avg("discount").over(w_recent).alias("last4_avg_discount"),
        F.count("discount").over(w_recent).alias("last4_count_discount"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (~F.col("x.status").isin(["SUCCESS", "FAILED"])), "left_semi")
)

result = (
    not_flagged
    .withColumn("last4_sum_discount_tier", F.when(F.col("last4_sum_discount") >= 1.77, "HIGH").otherwise("LOW"))
    .withColumn("last4_count_discount_tier", F.when(F.col("last4_count_discount") >= 14, "LARGE").otherwise("SMALL"))
    .cube("last4_sum_discount_tier", "last4_count_discount_tier")
    .agg(
        F.count_if(F.col("shipment_carrier") == "DHL").alias("dhl_events"),
        F.percentile_approx("discount", 0.9).alias("discount_p90"),
        F.round(F.avg("last4_avg_discount"), 2).alias("avg_last4_avg_discount"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
        F.min("discount").alias("min_discount"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("num_rows") > 1)
)
