from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc_nulls_last(), "id")

deduped = (
    df_order_items
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id")
)

enriched = (
    deduped
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .filter(F.col("d.store_id").isin([10, 23, 32]))
    .select("src.id", "src.order_id", F.col("d.amount"))
)

cleaned = (
    enriched
    .filter(F.col("amount").between(720, 900))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("amount_int", F.col("amount").cast("int"))
)

kept_keys = (
    cleaned
    .select("order_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") > 6)
        .select("order_id", "id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("amount") > 890)
    .withColumn("amount_filled_whole", F.floor("amount_filled"))
)

with_dims = (
    valid
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select(
        "s.order_id",
        "s.amount",
        "s.amount_filled",
        "s.amount_int",
        F.col("p.shipping_fee"),
        F.col("p.discount"),
    )
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("amount_filled") >= 80, "LARGE").otherwise("SMALL").alias("amount_filled_tier"),
    )
    .agg(
        F.percentile_approx("shipping_fee", 0.95).alias("shipping_fee_p95"),
        F.percentile_approx("shipping_fee", 0.95).alias("shipping_fee_p95_2"),
        F.count("*").alias("row_count"),
        F.avg("amount_int").alias("avg_amount_int"),
        F.percentile_approx("discount", 0.95).alias("discount_p95"),
    )
)
