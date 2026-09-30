from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w4 = Window.partitionBy("product_id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.corr("unit_price", "quantity").over(w).alias("corr_unit_price_quantity"),
        F.first("unit_price", ignorenulls=True).over(w2).alias("first_known_unit_price"),
        F.row_number().over(w3).alias("seq_no"),
        F.count("unit_price").over(w4).alias("product_id_count_unit_price"),
    )
)
