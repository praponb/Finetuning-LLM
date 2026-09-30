from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("unit_price").asc_nulls_last())

result = (
    df_order_items
    .filter(F.col("order_id") == 12)
    .select(
        "*",
        F.min("unit_price").over(w).alias("running_min_unit_price"),
        F.avg("unit_price").over(w).alias("running_avg_unit_price"),
        F.sum("unit_price").over(w).alias("running_sum_unit_price"),
        F.count("unit_price").over(w).alias("running_count_unit_price"),
        F.cume_dist().over(w2).alias("unit_price_cume_dist"),
    )
)
