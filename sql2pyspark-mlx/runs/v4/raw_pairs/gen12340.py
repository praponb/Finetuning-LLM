from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id")
w2 = Window.orderBy("amount_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("order_id") == 27))
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        F.round((F.col("amount") * 1.07), 2).alias("amount_scaled"),
        F.ceil("amount").alias("amount_whole"),
        F.coalesce("method", F.lit("NONE")).alias("method_clean"),
    )
)

with_windows = (
    filtered
    .withColumn("amount_bucket", F.ntile(4).over(w))
    .withColumn("cumulative_amount", F.sum("amount").over(w2))
    .withColumn("cumulative_amount_2", F.sum("amount").over(w3))
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select("s.*", F.col("o.discount"), F.col("o.status").alias("order_status"))
)

result = (
    with_dims
    .select(
        "id",
        "amount",
        "amount_scaled",
        "amount_bucket",
        "cumulative_amount",
        "cumulative_amount_2",
        "discount",
    )
)
