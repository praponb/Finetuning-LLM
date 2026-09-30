from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc(), "id")
w2 = Window.partitionBy("store_id", "status").orderBy("shipping_fee", "id")
w_ordered = Window.partitionBy("status", "customer_id").orderBy("discount", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy(F.col("discount").desc(), "id")
w_hist = Window.partitionBy("customer_id").orderBy(F.col("discount").asc_nulls_first())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("customer_id").orderBy("next_shipping_fee", "id")
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.max("units").alias("max_store_sale_units"))

scored = (
    df_orders
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("next_shipping_fee", F.lead("shipping_fee", 1).over(w2))
)

with_windows = (
    scored
    .withColumn("discount_avg_6", F.avg("discount").over(w_ordered))
    .withColumn("discount_bucket", F.ntile(3).over(w4))
    .withColumn("discount_dense_rank", F.dense_rank().over(w_hist))
)

with_last = (
    with_windows
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.paid_at"), F.col("c.method"))
)

latest = (
    with_last
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "row_seq",
        "next_shipping_fee",
        F.col("max_store_sale_units"),
    )
    .orderBy(F.col("shipping_fee").desc(), F.col("store_id").desc())
)
