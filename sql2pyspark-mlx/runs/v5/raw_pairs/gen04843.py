from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w_hist = Window.orderBy("quantity", "id")
w3 = Window.partitionBy("order_id", "product_id").orderBy(F.col("next_quantity").asc_nulls_last(), "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("next_unit_price").desc_nulls_first())
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.avg("quantity").alias("avg_order_item_quantity"))

enriched = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        F.lead("quantity").over(w_part).alias("next_quantity"),
        F.lead("unit_price").over(w_part).alias("next_unit_price"),
        F.lag("unit_price").over(w_part).alias("prev_unit_price"),
        F.row_number().over(w_part).alias("event_seq"),
        F.lag("quantity", 2).over(w_hist).alias("prev_quantity"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    deduped
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
)

matched_keys = (
    latest
    .select("order_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([3, 14, 30, 37]))
        .select("order_id", "id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "next_unit_price",
        "prev_quantity",
        "rn",
        F.col("avg_order_item_quantity"),
    )
    .orderBy(F.col("product_id").desc())
)
