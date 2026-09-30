from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("discount").desc())
w_grp = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("store_id").orderBy(F.col("amount").asc_nulls_last())
w4 = Window.partitionBy("status", "customer_id").orderBy(F.col("amount").asc_nulls_last())
w5 = Window.partitionBy("customer_id").orderBy("amount", "id")

result = (
    df_orders
    .select(
        "id",
        "order_date",
        F.dense_rank().over(w).alias("discount_dense_rank"),
        F.first("discount", ignorenulls=True).over(w_grp).alias("first_known_discount"),
        F.percent_rank().over(w_recent).alias("amount_percent_rank"),
        F.dense_rank().over(w4).alias("amount_dense_rank"),
        F.ntile(10).over(w5).alias("amount_bucket"),
    )
)
