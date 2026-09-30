from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "customer_id").orderBy(F.col("order_date").desc())

latest = (
    df_orders
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

not_flagged = (
    latest
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "left_anti")
)

with_window = (
    not_flagged
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.order_date") >= F.col("pr.starts_on")) & (F.col("a.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

filtered = (
    with_window
    .filter(F.col("store_id").isNotNull())
    .select(
        "*",
        F.least("amount", "discount_pct").alias("least_amount_discount_pct"),
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
        F.coalesce("promo_code", F.lit("UNKNOWN")).alias("resolved_promo_code"),
    )
)

result = (
    filtered
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.36, "PREMIUM").otherwise("STANDARD"))
    .withColumn("days_since_order_date_tier", F.when(F.col("days_since_order_date") >= 85, "PREMIUM").otherwise("STANDARD"))
    .cube("discount_pct_tier", "days_since_order_date_tier")
    .agg(
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.min("discount").alias("min_discount"),
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_rows"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("min_discount") < 0)
    .orderBy(F.col("vip_rows").asc_nulls_first())
)
