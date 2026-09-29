from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_status", "order_id").orderBy("paid_at", "id")
w_ordered = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "order_status").orderBy("paid_at", "id").rowsBetween(-5, Window.currentRow)

base = (
    df_payments
    .filter(F.col("amount") < 1560)
    .select(
        "*",
        F.substring("status", 1, 3).alias("status_prefix"),
        F.initcap(F.trim("method")).alias("method_title"),
        F.coalesce("method", F.lit("DIRECT")).alias("resolved_method"),
    )
)

joined = (
    base
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.store_id") == 9)
    .select(
        "e.id",
        "e.order_id",
        "e.method",
        "e.amount",
        "e.paid_at",
        "e.status",
        "e.resolved_method",
        F.col("d.channel"),
        F.col("d.discount"),
        F.col("d.status").alias("order_status"),
    )
)

deduped = (
    joined
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "resolved_method",
        "channel",
        "discount",
        "order_status",
    )
)

scored = (
    deduped
    .select(
        "*",
        F.first("method", ignorenulls=True).over(w_ordered).alias("first_known_method"),
        F.avg("discount").over(w3).alias("discount_avg_5"),
    )
)

grouped = (
    scored
    .cube("channel", "order_status")
    .agg(
        F.countDistinct("method").alias("distinct_methods"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("discount_avg_5")).otherwise(0)).alias("bank_transfer_discount_avg_5"),
        F.percentile_approx("discount_avg_5", 0.5).alias("discount_avg_5_p50"),
        F.max("paid_at").alias("last_seen"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_rows"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    grouped
    .select(
        "channel",
        "order_status",
        "bank_transfer_discount_avg_5",
        "discount_avg_5_p50",
        "last_seen",
        "gid",
    )
)
