from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy("order_date", "id", "promo_id")
w_running = Window.partitionBy("status", "channel").orderBy("discount", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("shipping_fee").asc_nulls_last())

latest_match = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("payment_amount"), F.col("c.method"))
)

matched = (
    latest_match
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

enriched = (
    matched
    .select(
        "*",
        F.row_number().over(w2).alias("row_seq"),
        F.sum("discount").over(w_running).alias("cumulative_discount"),
        F.dense_rank().over(w4).alias("shipping_fee_dense_rank"),
    )
)

result = (
    enriched
    .groupBy("store_id")
    .agg(
        F.count(F.when(F.col("promo_code") == "BUNDLE", 1)).alias("bundle_count"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 1)
    .orderBy(F.col("row_count").asc_nulls_first())
)
