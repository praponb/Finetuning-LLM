from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price_int").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price_int", "id")
w3 = Window.partitionBy("product_id").orderBy("row_num", "id")
w4 = Window.partitionBy("order_id").orderBy("prev_unit_price_int", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.orderBy(F.col("prev_unit_price_int").desc_nulls_last())
w6 = Window.partitionBy("product_id", "order_id").orderBy(F.col("event_seq").asc_nulls_first())
w7 = Window.orderBy("unit_price_avg_5_plus_event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("next_quantity", "id")
w_hist = Window.orderBy("next_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("product_id") == 35))
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    latest
    .withColumn("next_quantity", F.lead("quantity").over(w2))
    .withColumn("prev_unit_price_int", F.lag("unit_price_int").over(w2))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("prev_unit_price", F.lag("unit_price", 2, 0).over(w3))
)

scored_order_items = (
    scored
    .filter(F.col("product_id").isNull())
    .select(
        "id",
        "quantity",
        "unit_price",
        "row_num",
        "next_quantity",
        "prev_unit_price_int",
        "event_seq",
        "prev_unit_price",
        F.avg("unit_price").over(w4).alias("unit_price_avg_5"),
        F.cume_dist().over(w5).alias("prev_unit_price_int_cume_dist"),
        F.rank().over(w6).alias("event_seq_rank"),
    )
)

valid = (
    scored_order_items
    .filter(F.col("prev_unit_price_int_cume_dist") <= 0.31)
    .select(
        "id",
        "quantity",
        "unit_price",
        "row_num",
        "next_quantity",
        "prev_unit_price_int",
        "event_seq",
        "prev_unit_price",
        "event_seq_rank",
        (F.col("unit_price_avg_5") + F.col("event_seq")).alias("unit_price_avg_5_plus_event_seq"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "next_quantity",
        "prev_unit_price_int",
        "event_seq",
        "event_seq_rank",
        "unit_price_avg_5_plus_event_seq",
        F.last("prev_unit_price_int", ignorenulls=True).over(w7).alias("last_known_prev_unit_price_int"),
        F.ntile(5).over(w8).alias("next_quantity_bucket"),
        F.sum("quantity").over(w_hist).alias("cumulative_quantity"),
    )
)

grouped = (
    metrics
    .groupBy(
        F.when(F.col("last_known_prev_unit_price_int") >= 88, "PREMIUM").otherwise("STANDARD").alias("last_known_prev_unit_price_int_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("cumulative_quantity", 0.5).alias("cumulative_quantity_p50"),
        F.round(F.avg("last_known_prev_unit_price_int"), 2).alias("avg_last_known_prev_unit_price_int"),
        F.percentile_approx("unit_price_avg_5_plus_event_seq", 0.5).alias("unit_price_avg_5_plus_event_seq_p50"),
        F.round(F.avg("next_quantity"), 2).alias("avg_next_quantity"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .select(
        "last_known_prev_unit_price_int_tier",
        "cnt",
        "cumulative_quantity_p50",
        "unit_price_avg_5_plus_event_seq_p50",
        "avg_next_quantity",
    )
    .orderBy("last_known_prev_unit_price_int_tier")
)
