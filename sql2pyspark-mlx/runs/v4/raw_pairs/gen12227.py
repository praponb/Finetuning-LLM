from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("quantity_scaled_minus_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("quantity_scaled_minus_cost", "id")
w_recent = Window.partitionBy("product_id").orderBy("quantity_scaled_minus_cost", "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_scaled"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.cost"))
)

valid = (
    enriched
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        (F.col("quantity_scaled") - F.col("cost")).alias("quantity_scaled_minus_cost"),
    )
)

result = (
    valid
    .withColumn("unit_price_bucket", F.ntile(3).over(w_hist))
    .withColumn("running_quantity_scaled", F.sum("quantity_scaled").over(w2))
    .withColumn("prev_quantity_scaled", F.lag("quantity_scaled", 1, 0).over(w3))
    .withColumn("row_seq", F.row_number().over(w_recent))
)
