from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("delivered_date", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("discount", "id", "promo_id").rowsBetween(-6, -1)

valid = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-04-29"))) & (~F.col("carrier").eqNullSafe(F.lit("DHL"))))
    .select(
        "*",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.coalesce("carrier", F.lit("N/A")).alias("resolved_carrier"),
    )
)

joined = (
    valid
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.customer_id").isin([18, 24, 38]))
    .select("a.*", F.col("d.channel"), F.col("d.amount"), F.col("d.discount"))
)

with_window = (
    joined
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & F.col("e.shipped_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

metrics = (
    with_window
    .select(
        "id",
        "carrier",
        "delivered_date",
        "status",
        "delivered_date_month_end",
        "amount",
        "discount",
        "promo_id",
        "promo_code",
        "discount_pct",
        F.min("cost").over(w).alias("running_min_cost"),
        F.avg("cost").over(w).alias("running_avg_cost"),
        F.max("cost").over(w).alias("running_max_cost"),
        F.sum("cost").over(w).alias("running_sum_cost"),
        F.avg("discount").over(w2).alias("discount_avg_6_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "carrier",
        "delivered_date",
        "status",
        "delivered_date_month_end",
        "discount",
        "promo_id",
        "promo_code",
        "discount_pct",
        F.when(F.col("status").rlike("^(LOST|IN_TRANSIT)$"), "GROUP_A").when(F.col("status").rlike("^(DELIVERED|RETURNED)$"), "PRIORITY").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("status_zone").desc_nulls_first())
)
