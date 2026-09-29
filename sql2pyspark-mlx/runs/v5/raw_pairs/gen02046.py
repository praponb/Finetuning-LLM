from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id").orderBy("unit_price_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("least_quantity_unit_price", F.least("quantity", "unit_price"))
    .withColumn("unit_price_filled", F.coalesce("unit_price", F.lit(0)))
)

result = (
    filtered
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "least_quantity_unit_price",
        F.sum("least_quantity_unit_price").over(w_running).alias("running_sum_least_quantity_unit_price"),
        F.max("least_quantity_unit_price").over(w_running).alias("running_max_least_quantity_unit_price"),
        F.min("least_quantity_unit_price").over(w_running).alias("running_min_least_quantity_unit_price"),
        F.count("least_quantity_unit_price").over(w_running).alias("running_count_least_quantity_unit_price"),
    )
)
