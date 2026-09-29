from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        F.count("unit_price").over(w).alias("last3_count_unit_price"),
        F.min("unit_price").over(w).alias("last3_min_unit_price"),
        F.max("unit_price").over(w).alias("last3_max_unit_price"),
        F.sum("unit_price").over(w).alias("last3_sum_unit_price"),
    )
)
