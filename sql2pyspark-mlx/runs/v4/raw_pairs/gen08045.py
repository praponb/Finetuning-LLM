from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("amount").desc(), "id")
w_grp = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(-2, -1)
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

scored = (
    df_payments
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("last_known_method", F.last("method", ignorenulls=True).over(w_grp))
    .withColumn("amount_avg_2_prior", F.avg("amount").over(w3))
)

result = (
    scored
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "event_seq",
        "last_known_method",
        F.when(F.col("method").rlike("^(promptpay|cash)$"), "GROUP_B").when(F.col("method").rlike("^(card|paypal)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("method_zone"),
        F.col("avg_shipment_cost"),
    )
    .orderBy(F.col("method_zone").desc_nulls_first())
)
