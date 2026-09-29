from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "promo_code").orderBy("promo_id", "id", "promo_id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy(F.col("order_date").desc())
w3 = Window.orderBy("amount_int")
w4 = Window.partitionBy("promo_code").orderBy("amount_filled", "id", "promo_id")
w5 = Window.partitionBy("channel").orderBy("order_date", "id", "promo_id")

valid = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-05-08").cast("date"))
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

with_window = (
    valid
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.order_date") >= F.col("pr.starts_on")) & (F.col("s.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

metrics = (
    with_window
    .select(
        "*",
        F.min("amount_int").over(w).alias("last7_min_amount_int"),
        F.sum("amount_int").over(w).alias("last7_sum_amount_int"),
        F.max("amount_int").over(w).alias("last7_max_amount_int"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

kept = (
    ranked
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") < F.col("s.order_date")), "left_semi")
)

with_windows = (
    kept
    .select(
        "id",
        "order_date",
        "status",
        "discount",
        "amount_filled",
        "promo_id",
        "promo_code",
        "last7_min_amount_int",
        F.percent_rank().over(w3).alias("amount_int_percent_rank"),
        F.lead("discount", 1, 0).over(w4).alias("next_discount"),
        F.lag("discount", 2).over(w5).alias("prev_discount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "promo_id",
        "status",
        F.col("next_discount").alias("NEXT_DISCOUNT"),
        F.col("amount_filled").alias("AMOUNT_FILLED_VAL"),
    )
    .unpivot(
        ids=["id", "promo_id", "status"],
        values=["NEXT_DISCOUNT", "AMOUNT_FILLED_VAL"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
