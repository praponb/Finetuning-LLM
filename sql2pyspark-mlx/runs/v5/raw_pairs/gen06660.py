from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("order_date", "id")
w4 = Window.partitionBy("status").orderBy(F.col("discount").desc_nulls_first())
w5 = Window.partitionBy("channel").orderBy("discount_avg_2", "id")
w6 = Window.partitionBy("channel").orderBy(F.col("prev_amount").desc(), "id")
w7 = Window.partitionBy("customer_id", "store_id")

ranked = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    ranked
    .filter(F.col("rn") <= 1)
    .select(
        "*",
        F.avg("discount").over(w2).alias("discount_avg_2"),
        F.lag("amount", 1).over(w3).alias("prev_amount"),
        F.percent_rank().over(w4).alias("discount_percent_rank"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.lag("discount", 1, 0).over(w5).alias("prev_discount"),
        F.row_number().over(w6).alias("row_seq"),
        F.count("amount").over(w7).alias("customer_id_count_amount"),
    )
)

with_window = (
    with_windows
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

stats = (
    with_window
    .rollup(
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
        F.when(F.col("amount") >= 1650, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.percentile_approx("customer_id_count_amount", 0.95).alias("customer_id_count_amount_p95"),
        F.sum("amount").alias("total_amount"),
        F.percentile_approx("promo_id", 0.95).alias("promo_id_p95"),
    )
)

result = (
    stats
    .select("activity_month", "amount_tier", "promo_id_p95")
    .orderBy(F.col("amount_tier").desc())
)
