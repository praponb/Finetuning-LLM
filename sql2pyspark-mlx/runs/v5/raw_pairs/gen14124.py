from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "customer_id")
w2 = Window.partitionBy("store_id").orderBy("amount", "id")
w3 = Window.partitionBy("customer_id", "store_id").orderBy("discount", "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")
w5 = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc_nulls_last())

with_windows = (
    df_orders
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.ntile(3).over(w2).alias("amount_bucket"),
        F.ntile(3).over(w3).alias("discount_bucket"),
        F.row_number().over(w4).alias("seq_no"),
        F.dense_rank().over(w5).alias("shipping_fee_dense_rank"),
    )
)

result = (
    with_windows
    .cube(F.when(F.col("seq_no") >= 9, "LARGE").otherwise("SMALL").alias("seq_no_tier"))
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
    .orderBy("seq_no_tier")
)
