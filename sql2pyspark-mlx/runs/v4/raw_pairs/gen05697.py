from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("line_total_2").asc_nulls_last())
w3 = Window.partitionBy("order_id").orderBy(F.col("line_total_2").desc(), "id")
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.max("quantity").alias("max_order_item_quantity"))

valid = (
    df_order_items
    .filter(F.col("order_id") == 33)
    .select(
        "*",
        F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

cleaned = (
    valid
    .filter(F.col("product_id").isNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "quantity_band",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        (F.col("unit_price") + F.col("quantity")).alias("line_total_2"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.last("line_total_2", ignorenulls=True).over(w).alias("last_known_line_total_2"),
        F.cume_dist().over(w2).alias("line_total_2_cume_dist"),
    )
)

latest = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    latest
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.product_name"), F.col("p.stock_qty"))
)

kept_keys = (
    joined
    .select("product_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("order_id") == 40)
        .select("product_id", "id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys = (
    reconciled
    .select("product_id", "order_id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity") < 5)
        .select("product_id", "order_id")
    )
)

reconciled_order_items = (
    reconciled
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    reconciled_order_items
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "line_total",
        "line_total_2",
        "last_known_line_total_2",
        "line_total_2_cume_dist",
        "dedup_rank",
        F.col("max_order_item_quantity"),
    )
    .orderBy(F.col("dedup_rank").desc(), "last_known_line_total_2")
)
