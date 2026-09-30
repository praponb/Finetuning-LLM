from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id").orderBy("unit_price_adjusted", "id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("order_id").orderBy("seq_no", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_order_items
    .filter(F.col("quantity") != 3)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.round((F.col("unit_price") * 1.15), 2).alias("unit_price_adjusted"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.lead("unit_price").over(w_running).alias("next_unit_price"),
        F.lead("unit_price_adjusted").over(w_running).alias("next_unit_price_adjusted"),
        F.lag("unit_price_adjusted").over(w_running).alias("prev_unit_price_adjusted"),
        F.row_number().over(w_running).alias("seq_no"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

enriched_order_items = (
    enriched
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "unit_price_adjusted",
        "next_unit_price",
        "next_unit_price_adjusted",
        "prev_unit_price_adjusted",
        "seq_no",
        F.round((F.col("seq_no") / F.sum("seq_no").over(w2)), 4).alias("seq_no_share"),
        F.avg("prev_unit_price_adjusted").over(w3).alias("prev_unit_price_adjusted_avg_2"),
    )
)

matched_keys = (
    enriched_order_items
    .select("id")
    .exceptAll(
        df_order_items
        .filter(F.col("order_id").isin([13, 29, 37, 40]))
        .select("id")
    )
)

verified = (
    enriched_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "quantity",
        "unit_price",
        "unit_price_adjusted",
        "next_unit_price",
        "next_unit_price_adjusted",
        "seq_no",
        "prev_unit_price_adjusted_avg_2",
    )
)
