from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("channel")
w4 = Window.partitionBy("customer_id", "store_id").orderBy("amount_bucket", "id", "promo_id")

base = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "*",
        F.concat_ws("/", "status", "channel").alias("status_channel_key"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.last("channel", ignorenulls=True).over(w_grp).alias("last_known_channel"),
        F.ntile(5).over(w2).alias("amount_bucket"),
        F.count("shipping_fee").over(w3).alias("channel_count_shipping_fee"),
    )
)

attributed = (
    metrics
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") <= F.date_add(F.col("pr.ends_on"), 3)), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

deduped = (
    attributed
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .withColumn("quarter_start", F.date_trunc("QUARTER", "order_date"))
    .groupingSets(
        [["quarter_start", "status"], ["quarter_start"]],
        "quarter_start",
        "status",
    )
    .agg(
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.corr("shipping_fee", "amount").alias("corr_shipping_fee_amount"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("promo_code") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.grouping("quarter_start").alias("grouping_level"),
    )
    .orderBy("corr_shipping_fee_amount")
)
