from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("method", "order_id").orderBy(F.col("amount").desc())
w3 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-5, -1)
w4 = Window.orderBy(F.col("amount_avg_5_prior").desc_nulls_last())
w5 = Window.partitionBy("order_id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

scored = (
    df_payments
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("amount_cume_dist", F.cume_dist().over(w2))
    .withColumn("amount_avg_5_prior", F.avg("amount").over(w3))
)

base = (
    scored
    .filter((F.col("paid_at") >= F.lit("2023-05-31").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("amount_avg_5_prior_plus_amount_cume_dist", (F.col("amount_avg_5_prior") + F.col("amount_cume_dist")))
)

joined = (
    base
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .filter(F.col("p.discount") < 0.15)
    .select(
        "src.id",
        "src.order_id",
        "src.method",
        "src.amount",
        "src.paid_at",
        "src.status",
        "src.row_seq",
        "src.amount_cume_dist",
        "src.amount_avg_5_prior",
        "src.amount_avg_5_prior_plus_amount_cume_dist",
        F.col("p.order_date"),
        F.col("p.shipping_fee"),
        F.col("p.channel"),
    )
)

remaining_keys = (
    joined
    .select("id")
    .subtract(
        df_payments
        .filter(F.col("order_id").isin([17, 18]))
        .select("id")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("amount_cume_dist").between(0.56, 0.77))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "row_seq",
        "amount_cume_dist",
        "amount_avg_5_prior",
        "amount_avg_5_prior_plus_amount_cume_dist",
        "order_date",
        "shipping_fee",
        "channel",
        F.when(F.col("amount") >= 1410, "High").when(F.col("amount") >= 720, "Medium").otherwise("Low").alias("amount_level"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w4).alias("amount_avg_5_prior_dense_rank"),
        F.avg("amount_avg_5_prior").over(w5).alias("order_id_avg_amount_avg_5_prior"),
    )
)

matched = (
    enriched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.paid_at") >= F.col("pr.starts_on")) & (F.col("a.paid_at") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "row_seq",
        "amount_cume_dist",
        "amount_avg_5_prior_plus_amount_cume_dist",
        "channel",
        "amount_level",
        F.when(F.col("status").rlike("^(SUCCESS|REFUNDED)$"), "GROUP_B").when(F.col("status").rlike("^(FAILED)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("status_zone"),
        F.col("max_shipment_cost"),
    )
    .orderBy(F.col("channel").desc_nulls_last())
)
