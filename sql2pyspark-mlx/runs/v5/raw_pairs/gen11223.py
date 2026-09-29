from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount_filled").asc_nulls_last(), "id")

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-07-27 09:00:00"))) & (F.col("amount") >= 540))
    .select(
        "id",
        "order_id",
        "status",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.when(F.col("method") == "bank_transfer", 1).otherwise(0).alias("is_bank_transfer"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

kept_keys = (
    ranked
    .select("order_id", "status")
    .intersectAll(
        df_payments
        .filter(F.col("order_id").isNull())
        .select("order_id", "status")
    )
)

verified = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .filter(F.col("d.store_id").isNull())
    .select("s.id", "s.order_id", "s.resolved_method", F.col("d.discount"))
)

cleaned = (
    with_dims
    .filter(F.col("discount") < 0.35)
    .select(
        "*",
        F.when(F.col("discount") >= 0.36, "High").when(F.col("discount") >= 0.06, "Medium").otherwise("Low").alias("discount_level"),
    )
)

stats = (
    cleaned
    .groupBy("order_id")
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.count("*").alias("cnt"),
    )
)

result = (
    stats
    .select("order_id", "avg_discount", "cnt")
    .orderBy(F.col("cnt").desc_nulls_last())
)
