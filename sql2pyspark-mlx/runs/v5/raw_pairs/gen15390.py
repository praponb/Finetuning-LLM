from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("least_unit_price_quantity", "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
    )
)

filtered_order_items = (
    filtered
    .filter(F.col("quantity") == 3)
    .withColumn("least_unit_price_quantity_plus_quantity", (F.col("least_unit_price_quantity") + F.col("quantity")))
)

deduped = (
    filtered_order_items
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    deduped
    .select("id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([1, 29, 31, 32]))
        .select("id", "product_id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "least_unit_price_quantity",
        "least_unit_price_quantity_plus_quantity",
        "latest_rank",
    )
    .orderBy("unit_price", "product_id")
)
