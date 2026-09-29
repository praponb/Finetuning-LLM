from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("order_id").orderBy(F.col("unit_price").asc_nulls_first())
w3 = Window.partitionBy("order_id").orderBy("unit_price_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("order_id") == 5)
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.round((F.col("unit_price") * 100), 2).alias("unit_price_scaled"),
        F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

result = (
    valid
    .filter(F.col("product_id").isNull())
    .select(
        "id",
        "order_id",
        "unit_price",
        "unit_price_scaled",
        F.count("unit_price").over(w).alias("order_id_count_unit_price"),
        F.rank().over(w2).alias("unit_price_rank"),
        F.last("unit_price", ignorenulls=True).over(w3).alias("last_known_unit_price"),
    )
)
