from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("amount").desc(), "id")
w3 = Window.orderBy("order_date", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("status").orderBy("amount_int")
w5 = Window.partitionBy("order_id").orderBy("shipping_fee", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("amount", "id")
w7 = Window.partitionBy("status").orderBy(F.col("order_amount").desc(), "id")

base = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-10-11 20:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "method",
        "status",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.order_date"), F.col("d.shipping_fee"), F.col("d.amount"))
)

scored = (
    joined
    .withColumn("first_known_method", F.first("method", ignorenulls=True).over(w))
    .withColumn("amount_bucket", F.ntile(5).over(w2))
)

with_dims = (
    scored
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.amount").alias("order_amount"))
)

result = (
    with_dims
    .select(
        "*",
        F.avg("amount").over(w3).alias("amount_avg_6_prior"),
        F.dense_rank().over(w4).alias("amount_int_dense_rank"),
        F.avg("amount_bucket").over(w5).alias("amount_bucket_avg_3"),
        F.lag("order_amount", 1).over(w6).alias("prev_order_amount"),
        F.ntile(4).over(w7).alias("order_amount_bucket"),
    )
)
