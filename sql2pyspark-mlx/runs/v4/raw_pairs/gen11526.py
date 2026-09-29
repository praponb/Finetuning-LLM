from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("paid_at").desc(), "id")
w2 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method").orderBy(F.col("last_known_amount").desc(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .withColumn("method_filled", F.coalesce("method", F.lit("unassigned")))
)

with_windows = (
    valid
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("amount_max_30d", F.max("amount").over(w2))
    .withColumn("amount_avg_6", F.avg("amount").over(w3))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w4))
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "method_filled",
        "seq_no",
        "amount_max_30d",
        "amount_avg_6",
        F.col("sum_shipment_cost"),
    )
    .orderBy(F.col("seq_no").asc_nulls_first(), "amount_max_30d")
)
