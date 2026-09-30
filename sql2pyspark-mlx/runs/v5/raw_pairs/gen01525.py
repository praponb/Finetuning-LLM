from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter(F.year("order_date") == 2025)
    .select(
        "*",
        F.concat("status", F.lit(", "), "channel").alias("status_channel"),
        F.concat_ws("/", "status", "channel").alias("status_channel_key"),
    )
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "status_channel",
    )
)

latest_match = (
    latest
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.paid_at"))
)

eligible = (
    latest_match
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") < F.col("s.order_date")), "left_semi")
)

matched = (
    eligible
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.order_date") >= F.col("pr.starts_on")) & (F.col("a.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "discount",
        "status_channel",
        "paid_at",
        "promo_id",
        "promo_code",
        F.when(F.col("status").rlike("^(CANCELLED|PENDING)$"), "CORE").when(F.col("status").rlike("^(COMPLETED|SHIPPED)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("promo_id").desc_nulls_last())
)
