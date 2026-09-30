from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("stock_qty").asc_nulls_first())
w2 = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(-2, -1)

result = (
    df_products
    .select(
        "*",
        F.dense_rank().over(w).alias("stock_qty_dense_rank"),
        F.first("price", ignorenulls=True).over(w2).alias("first_known_price"),
        F.avg("stock_qty").over(w_hist).alias("stock_qty_avg_2_prior"),
    )
)
