from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.max("price").over(w).alias("running_max_price"),
        F.sum("price").over(w).alias("running_sum_price"),
        F.count("price").over(w).alias("running_count_price"),
    )
)
