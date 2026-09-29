from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w3 = Window.orderBy("quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_rounded", F.round("quantity", 0))
)

scored = (
    base
    .select(
        "*",
        F.row_number().over(w_hist).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
        F.first("quantity_rounded", ignorenulls=True).over(w3).alias("first_known_quantity_rounded"),
        F.sum("quantity_rounded").over(w4).alias("cumulative_quantity_rounded"),
    )
)

enriched = (
    scored
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.order_date"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.amount"))
)

matched_keys = (
    with_dims
    .select("id")
    .intersectAll(
        df_order_items
        .filter(F.col("unit_price") > 330)
        .select("id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    kept_rows
    .select("id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price") <= 240)
        .select("id", "product_id")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

grouped = (
    reconciled
    .groupBy(
        "product_id",
        F.when(F.col("event_seq_2") >= 7, "PREMIUM").otherwise("STANDARD").alias("event_seq_2_tier"),
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.min("event_seq").alias("min_event_seq"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.min("first_known_quantity_rounded").alias("min_first_known_quantity_rounded"),
        F.countDistinct("order_id").alias("distinct_order_ids_2"),
    )
)

result = (
    grouped
    .select(
        "product_id",
        "event_seq_2_tier",
        "min_event_seq",
        "min_first_known_quantity_rounded",
    )
)
