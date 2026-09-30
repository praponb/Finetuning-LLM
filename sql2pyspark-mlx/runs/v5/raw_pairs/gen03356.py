from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("store_id").orderBy("promo_id", "id", "promo_id").rowsBetween(-6, -1)
w4 = Window.partitionBy("carrier").orderBy("payment_paid_at", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_latest = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.amount").alias("payment_amount"),
        F.col("c.paid_at").alias("payment_paid_at"),
    )
)

matched = (
    with_latest
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.payment_paid_at") >= F.col("pr.starts_on")) & (F.col("src.payment_paid_at") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

latest_match = (
    matched
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") <= F.col("s.payment_paid_at")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.order_date",
        "s.status",
        "s.channel",
        "s.discount",
        "s.shipping_fee",
        "s.payment_paid_at",
        "s.promo_id",
        "s.promo_code",
        F.col("c.delivered_date"),
        F.col("c.carrier"),
    )
)

enriched = (
    latest_match
    .filter(F.to_date("payment_paid_at") >= "2024-03-01")
    .select(
        "*",
        F.avg("promo_id").over(w3).alias("promo_id_avg_6_prior"),
        F.last("promo_id", ignorenulls=True).over(w4).alias("last_known_promo_id"),
    )
)

result = (
    enriched
    .withColumn("quarter_start", F.date_trunc("QUARTER", "payment_paid_at"))
    .groupingSets(
        [["store_id", "quarter_start"], ["store_id"], []],
        "store_id",
        "quarter_start",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
        F.max("payment_paid_at").alias("last_payment_paid_at"),
    )
    .orderBy(F.col("cnt").desc())
)
