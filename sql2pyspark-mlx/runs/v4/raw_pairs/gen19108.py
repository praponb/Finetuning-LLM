from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("quantity_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("first_known_unit_price", "id")
w4 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.orderBy("row_seq", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("product_id").orderBy("unit_price_per_quantity_adjusted", "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([10, 19]))
    .select("*", F.round((F.col("quantity") * 1.15), 2).alias("quantity_adjusted"))
)

cleaned = (
    valid
    .filter(F.col("product_id").isin([27, 35, 37]))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "quantity_adjusted",
        (F.col("unit_price") / F.nullif("quantity_adjusted", F.lit(0))).alias("unit_price_per_quantity_adjusted"),
    )
)

enriched = (
    cleaned
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("first_known_unit_price", F.first("unit_price", ignorenulls=True).over(w2))
)

metrics = (
    enriched
    .withColumn("prev_row_seq", F.lag("row_seq", 1).over(w3))
    .withColumn("unit_price_avg_5", F.avg("unit_price").over(w4))
    .withColumn("quantity_avg_6_prior", F.avg("quantity").over(w5))
    .withColumn("unit_price_per_quantity_adjusted_bucket", F.ntile(10).over(w6))
)

result = (
    metrics
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity_adjusted",
        "unit_price_per_quantity_adjusted",
        "row_seq",
        "prev_row_seq",
        "unit_price_avg_5",
        "quantity_avg_6_prior",
    )
    .orderBy(F.col("prev_row_seq").desc_nulls_last())
)
