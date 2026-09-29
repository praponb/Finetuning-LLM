from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price_rounded").desc(), "id")

base = (
    df_order_items
    .filter(F.col("order_id").isin([11, 23]))
    .select(
        "id",
        "product_id",
        F.round("quantity", 1).alias("quantity_rounded"),
        F.round("unit_price", 0).alias("unit_price_rounded"),
    )
)

cleaned = (
    base
    .filter(F.col("unit_price_rounded") > 22)
    .withColumn("quantity_rounded_per_unit_price_rounded", (F.col("quantity_rounded") / F.nullif("unit_price_rounded", F.lit(0))))
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

cleaned_order_items = (
    latest
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "unit_price_rounded",
        "quantity_rounded_per_unit_price_rounded",
        "latest_rank",
        F.ceil("quantity_rounded").alias("quantity_rounded_whole"),
        F.round("quantity_rounded_per_unit_price_rounded", 1).alias("quantity_rounded_per_unit_price_rounded_rounded"),
    )
)

result = (
    cleaned_order_items
    .select(
        "id",
        "unit_price_rounded",
        "quantity_rounded_per_unit_price_rounded",
        "latest_rank",
        "quantity_rounded_whole",
    )
)
