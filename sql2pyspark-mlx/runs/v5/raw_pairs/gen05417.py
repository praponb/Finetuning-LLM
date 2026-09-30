from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc(), "id")
w_recent = Window.partitionBy("order_id").orderBy("shipping_fee", "id", "promo_id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_order_items
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

base = (
    ranked
    .filter(F.col("product_id").isin([12, 13]))
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "latest_rank",
        (F.col("latest_rank") * F.col("unit_price")).alias("latest_rank_times_unit_price"),
    )
)

with_dims = (
    base
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.channel"), F.col("d.order_date"), F.col("d.shipping_fee"))
)

attributed = (
    with_dims
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.order_date") >= F.col("pr.starts_on")) & (F.col("s.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

scored = (
    attributed
    .filter(F.col("latest_rank_times_unit_price") < 43)
    .select(
        "id",
        "quantity",
        "latest_rank",
        "latest_rank_times_unit_price",
        "order_date",
        "shipping_fee",
        "promo_id",
        F.max("unit_price").over(w_recent).alias("last7_max_unit_price"),
        F.min("unit_price").over(w_recent).alias("last7_min_unit_price"),
        F.sum("unit_price").over(w_recent).alias("last7_sum_unit_price"),
        F.count("unit_price").over(w_recent).alias("last7_count_unit_price"),
        F.last("latest_rank_times_unit_price", ignorenulls=True).over(w3).alias("last_known_latest_rank_times_unit_price"),
    )
)

grouped = (
    scored
    .groupBy("promo_id")
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("promo_id"), 2).alias("avg_promo_id"),
        F.min("last_known_latest_rank_times_unit_price").alias("min_last_known_latest_rank_times_unit_price"),
        F.avg("last_known_latest_rank_times_unit_price").alias("avg_last_known_latest_rank_times_unit_price"),
    )
)

result = (
    grouped
    .select(
        "promo_id",
        "quantity_bit_and",
        "avg_promo_id",
        "min_last_known_latest_rank_times_unit_price",
        "avg_last_known_latest_rank_times_unit_price",
    )
)
