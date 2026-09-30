from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w_grp = Window.orderBy("unit_price", "id")
w_ordered = Window.orderBy(F.col("event_seq").desc(), "id")
w_part = Window.orderBy("prev_row_seq", "id")
w5 = Window.orderBy("event_seq", "id")
w6 = Window.orderBy(F.col("prev_row_seq").desc(), "id")
w7 = Window.orderBy("next_quantity", "id")

with_windows = (
    df_order_items
    .select(
        "id",
        "unit_price",
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.lead("quantity").over(w).alias("next_quantity"),
        F.lag("unit_price").over(w).alias("prev_unit_price"),
        F.lead("unit_price").over(w).alias("next_unit_price"),
        F.row_number().over(w).alias("row_seq"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "next_quantity",
        "prev_unit_price",
        F.lag("row_seq").over(w_grp).alias("prev_row_seq"),
        F.lead("row_seq").over(w_grp).alias("next_row_seq"),
        F.lag("unit_price").over(w_grp).alias("prev_unit_price_2"),
        F.lead("unit_price").over(w_grp).alias("next_unit_price_2"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

scored = (
    metrics
    .withColumn("event_seq_bucket", F.ntile(4).over(w_ordered))
    .withColumn("prev_row_seq_bucket", F.ntile(10).over(w_part))
    .withColumn("event_seq_bucket_2", F.ntile(4).over(w5))
)

metrics_order_items = (
    scored
    .filter(F.col("event_seq_bucket") >= 4)
    .select(
        "id",
        "next_quantity",
        "next_row_seq",
        "prev_unit_price_2",
        "next_unit_price_2",
        "event_seq_bucket",
        "prev_row_seq_bucket",
        F.row_number().over(w6).alias("seq_no"),
        F.row_number().over(w7).alias("event_seq_2"),
    )
)

matched_keys = (
    metrics_order_items
    .select("id")
    .intersectAll(
        df_order_items
        .filter(F.col("order_id").isin([4, 17]))
        .select("id")
    )
)

kept_rows = (
    metrics_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("next_unit_price_2") >= 330, "PREMIUM").otherwise("STANDARD").alias("next_unit_price_2_tier"),
    )
    .agg(
        F.percentile_approx("prev_row_seq_bucket", 0.5).alias("prev_row_seq_bucket_p50"),
        F.round(F.avg("next_quantity"), 2).alias("avg_next_quantity"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
