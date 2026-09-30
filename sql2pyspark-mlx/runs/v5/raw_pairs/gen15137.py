from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("discount").desc_nulls_first())
w2 = Window.partitionBy("customer_id")

valid = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-08-07").cast("date")) & F.col("channel").isNotNull())
    .withColumn("channel_length", F.length("channel"))
    .withColumn("amount_with_shipping", (F.col("shipping_fee") - F.col("amount")))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("NONE")))
)

attributed = (
    valid
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.order_date") >= F.col("pr.starts_on")) & (F.col("a.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

scored = (
    attributed
    .filter(~F.col("promo_code").isin(["WELCOME5", "BUNDLE", "VIP"]))
    .select(
        "*",
        F.cume_dist().over(w).alias("discount_cume_dist"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
    )
)

matched = (
    scored
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & F.col("e.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .groupBy(F.date_trunc("week", "order_date").alias("week_start"))
    .agg(
        F.round(F.sum("promotion_discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("promotion_discount_pct_per_store"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
    )
)
