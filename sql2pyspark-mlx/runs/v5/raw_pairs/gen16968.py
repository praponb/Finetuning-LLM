from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id", "promo_id")
w2 = Window.orderBy("order_date", "id", "promo_id")
w3 = Window.partitionBy("promo_code").orderBy("discount", "id", "promo_id")
w4 = Window.partitionBy("promo_code").orderBy(F.col("discount").desc(), "id", "promo_id")
w5 = Window.partitionBy("promo_code")
w6 = Window.partitionBy("status").orderBy(F.col("prev_promo_id").desc(), "id", "promo_id")

filtered = (
    df_orders
    .filter(~F.col("channel").eqNullSafe(F.lit("web")))
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        (F.col("shipping_fee") * F.col("discount")).alias("shipping_fee_times_discount"),
        F.trim("status").alias("clean_status"),
    )
)

with_window = (
    filtered
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

scored = (
    with_window
    .select(
        "id",
        "status",
        "discount",
        "promo_id",
        "discount_pct",
        F.row_number().over(w).alias("event_seq"),
        F.lag("promo_id", 1).over(w2).alias("prev_promo_id"),
        F.lag("promo_id", 1).over(w3).alias("prev_promo_id_2"),
        F.ntile(3).over(w4).alias("discount_bucket"),
        F.corr("amount", "discount").over(w5).alias("corr_amount_discount"),
    )
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.delivered_date").between("2023-03-01", "2024-10-01")), "left_semi")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("discount") >= 0.41, "HIGH").otherwise("LOW").alias("discount_tier"),
    )
    .agg(
        F.count_if(F.col("status") == "COMPLETED").alias("completed_events"),
        F.stddev_samp("discount").alias("discount_stddev"),
        F.avg(F.when(F.col("status") == "CANCELLED", F.col("discount_bucket"))).alias("cancelled_avg_discount_bucket"),
    )
    .orderBy(F.col("discount_tier").asc_nulls_last())
)
