from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy("amount_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("amount_avg_6").desc(), "id")
w6 = Window.partitionBy("channel").orderBy("shipping_fee", "id")
w7 = Window.partitionBy("customer_id").orderBy("amount_avg_6_bucket", "id")
w8 = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("status")
w10 = Window.partitionBy("customer_id").orderBy(F.col("amount_avg_6_avg_6").asc_nulls_last())
w11 = Window.partitionBy("promo_code", "customer_id").orderBy(F.col("last_known_amount_times_discount").desc_nulls_first())
w12 = Window.partitionBy("channel")
w13 = Window.partitionBy("promo_code").orderBy("amount_avg_6", "id", "promo_id")
w14 = Window.partitionBy("promo_code", "customer_id").orderBy("last_known_amount_times_discount", "id", "promo_id")

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2024-05-05").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        (F.col("amount") * F.col("discount")).alias("amount_times_discount"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

scored = (
    base
    .select(
        "*",
        F.avg("amount_times_discount").over(w).alias("amount_times_discount_avg_2_prior"),
        F.avg("amount").over(w2).alias("amount_avg_6"),
    )
)

scored_orders = (
    scored
    .withColumn("amount_avg_6_avg_6", F.avg("amount_avg_6").over(w3))
    .withColumn("last_known_amount_times_discount", F.last("amount_times_discount", ignorenulls=True).over(w4))
    .withColumn("amount_avg_6_bucket", F.ntile(10).over(w5))
    .withColumn("next_amount_times_discount", F.lead("amount_times_discount", 1).over(w6))
)

enriched = (
    scored_orders
    .select(
        "*",
        F.row_number().over(w7).alias("event_seq"),
        F.first("amount_avg_6_bucket", ignorenulls=True).over(w8).alias("first_known_amount_avg_6_bucket"),
        F.round((F.col("amount_avg_6_bucket") / F.sum("amount_avg_6_bucket").over(w9)), 4).alias("amount_avg_6_bucket_share"),
        F.dense_rank().over(w10).alias("amount_avg_6_avg_6_dense_rank"),
        F.sum("last_known_amount_times_discount").over(w8).alias("running_last_known_amount_times_discount"),
    )
)

with_window = (
    enriched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & F.col("src.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_windows = (
    with_window
    .select(
        "*",
        F.cume_dist().over(w11).alias("last_known_amount_times_discount_cume_dist"),
        F.count("next_amount_times_discount").over(w12).alias("channel_count_next_amount_times_discount"),
        F.lead("amount_avg_6_avg_6_dense_rank", 1).over(w13).alias("next_amount_avg_6_avg_6_dense_rank"),
        F.row_number().over(w14).alias("seq_no"),
    )
)

result = (
    with_windows
    .groupBy("customer_id")
    .agg(
        F.avg(F.when(F.col("channel") == "partner", F.col("event_seq"))).alias("partner_avg_event_seq"),
        F.count_if(F.col("status") == "PENDING").alias("pending_events"),
        F.avg("event_seq").alias("avg_event_seq"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.round(F.avg("amount_avg_6_bucket"), 2).alias("avg_amount_avg_6_bucket"),
    )
    .orderBy(F.col("partner_avg_event_seq").asc_nulls_first())
)
