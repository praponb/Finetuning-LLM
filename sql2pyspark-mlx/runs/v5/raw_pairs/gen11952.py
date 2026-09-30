from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.orderBy(F.col("dedup_rank").desc(), "id")
w4 = Window.orderBy(F.col("dedup_rank_2").desc(), "id")
w5 = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").asc_nulls_last())
w_part = Window.orderBy("dedup_rank", "id")
w7 = Window.partitionBy("order_id").orderBy(F.col("prev_unit_price").asc_nulls_first(), "id")

latest = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank_2", F.row_number().over(w2))
    .filter(F.col("dedup_rank_2") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "order_id",
        "unit_price",
        "dedup_rank_2",
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("row_seq"),
        F.rank().over(w5).alias("quantity_rank"),
        F.lag("unit_price", 1).over(w_part).alias("prev_unit_price"),
    )
)

base = (
    with_windows
    .filter(F.col("dedup_rank_2") <= 1)
    .select(
        "id",
        "order_id",
        "dedup_rank_2",
        "event_seq",
        "row_seq",
        "quantity_rank",
        "prev_unit_price",
        F.round((F.col("dedup_rank_2") * 1.15), 2).alias("adjusted_dedup_rank_2"),
        (F.col("unit_price") + F.col("prev_unit_price")).alias("unit_price_plus_prev_unit_price"),
    )
)

latest_order_items = (
    base
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
    .select("id", "dedup_rank_2", "event_seq", "unit_price_plus_prev_unit_price")
)

result = (
    latest_order_items
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 8, "PREMIUM").otherwise("STANDARD"))
    .withColumn("unit_price_plus_prev_unit_price_tier", F.when(F.col("unit_price_plus_prev_unit_price") >= 90, "LARGE").otherwise("SMALL"))
    .cube("event_seq_tier", "unit_price_plus_prev_unit_price_tier")
    .agg(
        F.percentile_approx("event_seq", 0.9).alias("event_seq_p90"),
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.count("*").alias("event_count"),
        F.grouping("event_seq_tier").alias("gid"),
    )
    .filter(F.col("event_seq_p90") > 0)
    .orderBy(F.col("event_seq_p90").desc())
)
