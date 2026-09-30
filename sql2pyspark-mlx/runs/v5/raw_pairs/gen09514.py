from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy("days_since_order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "store_id")

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-03-10").cast("date")) & F.col("order_date").between("2023-11-01", "2024-05-01"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
    )
)

result = (
    cleaned
    .filter(F.col("customer_id").isNull())
    .withColumn("cumulative_days_since_order_date", F.sum("days_since_order_date").over(w_ordered))
    .withColumn("corr_amount_discount", F.corr("amount", "discount").over(w2))
)
