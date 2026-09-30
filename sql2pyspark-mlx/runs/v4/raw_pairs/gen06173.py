from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("promo_id").desc(), "id", "promo_id")

filtered = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-11-17").cast("date"))
    .withColumn("discount_times_amount", (F.col("discount") * F.col("amount")))
)

eligible = (
    filtered
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "left_anti")
)

attributed = (
    eligible
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

ranked = (
    attributed
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select(
        "id",
        "store_id",
        "order_date",
        "amount",
        "discount",
        "shipping_fee",
        "promo_id",
        "promo_code",
        "discount_pct",
    )
)
