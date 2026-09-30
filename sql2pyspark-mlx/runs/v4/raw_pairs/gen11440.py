from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount", "id")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("store_id", "status")
w_grp = Window.partitionBy("channel", "store_id").orderBy(F.col("discount").desc(), "id")

scored = (
    df_orders
    .select(
        "*",
        F.ntile(4).over(w).alias("discount_bucket"),
        F.corr("shipping_fee", "amount").over(w2).alias("corr_shipping_fee_amount"),
        F.corr("amount", "discount").over(w3).alias("corr_amount_discount"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

result = (
    scored
    .groupBy("store_id")
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count_2"),
    )
    .orderBy(F.col("returned_count").asc_nulls_last())
)
