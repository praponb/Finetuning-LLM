from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").asc_nulls_first())
w_part = Window.orderBy(F.col("amount").desc_nulls_first())
w3 = Window.partitionBy("city").orderBy(F.col("shipping_fee").desc_nulls_last())
w4 = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc())
w5 = Window.orderBy("signup_date", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("segment").orderBy("shipping_fee", "id")
w7 = Window.orderBy("amount", "id")

with_windows = (
    df_orders
    .withColumn("amount_rank", F.rank().over(w))
    .withColumn("amount_dense_rank", F.dense_rank().over(w_part))
)

with_dims = (
    with_windows
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.signup_date"), F.col("d.city"), F.col("d.segment"))
)

scored = (
    with_dims
    .select(
        "*",
        F.percent_rank().over(w3).alias("shipping_fee_percent_rank"),
        F.cume_dist().over(w4).alias("shipping_fee_cume_dist"),
        F.avg("amount_dense_rank").over(w5).alias("amount_dense_rank_avg_2"),
        F.lag("discount", 2).over(w6).alias("prev_discount"),
        F.ntile(5).over(w7).alias("amount_bucket"),
    )
)

result = (
    scored
    .select("id", "channel", "shipping_fee", "prev_discount")
    .unpivot(
        ["id", "channel"],
        ["shipping_fee", "prev_discount"],
        "metric_name",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
