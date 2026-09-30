from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc_nulls_last())
w_running = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(-5, -1)
w_hist = Window.partitionBy("category", "brand").orderBy(F.col("launch_date").desc(), "id")
w4 = Window.orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .select(
        "id",
        "price",
        "cost",
        "tags",
        F.dense_rank().over(w).alias("cost_dense_rank"),
        F.avg("price").over(w_running).alias("price_avg_5_prior"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.dense_rank().over(w4).alias("stock_qty_dense_rank"),
    )
)
