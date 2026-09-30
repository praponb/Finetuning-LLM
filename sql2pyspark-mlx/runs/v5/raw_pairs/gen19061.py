from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipping_fee", "id")
w2 = Window.orderBy("discount", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("amount").desc(), "id")
w4 = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("amount").desc_nulls_first())

result = (
    df_orders
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.avg("discount").over(w2).alias("discount_avg_5"),
        F.ntile(3).over(w3).alias("amount_bucket"),
        F.sum("discount").over(w4).alias("cumulative_discount"),
        F.dense_rank().over(w5).alias("amount_dense_rank"),
    )
)
