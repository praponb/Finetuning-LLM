from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("promo_id", "id", "promo_id")
w2 = Window.orderBy("amount", "id", "promo_id")
w3 = Window.partitionBy("promo_code").orderBy("shipping_fee", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("shipping_fee", "id", "promo_id").rowsBetween(-6, -1)
w5 = Window.partitionBy("customer_id").orderBy(F.col("discount_pct").desc(), "id", "promo_id")
w6 = Window.partitionBy("promo_code").orderBy("discount_pct", "id", "promo_id")
w_grp = Window.partitionBy("promo_code").orderBy("amount_avg_6_prior", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_bucket").asc_nulls_last())
w9 = Window.partitionBy("promo_code").orderBy(F.col("promo_id_bucket").asc_nulls_first())
w10 = Window.partitionBy("promo_id").orderBy(F.col("avg_discount_pct_bucket_percent_rank").desc())

kept = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.unit_price") < 120), "left_semi")
)

matched = (
    kept
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

scored = (
    matched
    .select(
        "id",
        "order_date",
        "amount",
        "discount",
        "promo_id",
        "promo_code",
        "discount_pct",
        F.ntile(4).over(w).alias("promo_id_bucket"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
        F.last("channel", ignorenulls=True).over(w3).alias("last_known_channel"),
        F.avg("amount").over(w4).alias("amount_avg_6_prior"),
        F.ntile(5).over(w5).alias("discount_pct_bucket"),
    )
)

with_windows = (
    scored
    .filter(F.col("discount") > 0.36)
    .select(
        "id",
        "amount",
        "promo_id",
        "promo_code",
        "next_amount",
        "last_known_channel",
        "amount_avg_6_prior",
        "discount_pct_bucket",
        F.lag("amount", 2).over(w6).alias("prev_amount"),
        F.sum("next_amount").over(w_grp).alias("cumulative_next_amount"),
        F.percent_rank().over(w_ordered).alias("discount_pct_bucket_percent_rank"),
        F.rank().over(w9).alias("promo_id_bucket_rank"),
        F.sum("next_amount").over(w_grp).alias("running_next_amount"),
    )
)

result = (
    with_windows
    .groupBy(
        "promo_id",
        F.when(F.col("discount_pct_bucket_percent_rank") >= 0.65, "LARGE").otherwise("SMALL").alias("discount_pct_bucket_percent_rank_tier"),
    )
    .agg(
        F.sum(F.when(F.col("promo_code") == "BUNDLE", F.col("promo_id")).otherwise(0)).alias("bundle_promo_id"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.count(F.when(F.col("promo_code") == "BUNDLE", 1)).alias("bundle_rows"),
        F.round(F.avg("discount_pct_bucket_percent_rank"), 2).alias("avg_discount_pct_bucket_percent_rank"),
        F.percentile_approx("running_next_amount", 0.9).alias("running_next_amount_p90"),
    )
    .withColumn("avg_discount_pct_bucket_percent_rank_percent_rank", F.percent_rank().over(w10))
    .orderBy(F.col("avg_discount_pct_bucket_percent_rank").desc())
)
