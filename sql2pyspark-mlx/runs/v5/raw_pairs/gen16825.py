from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "status").orderBy("shipped_date", "id")
w4 = Window.partitionBy("carrier", "status").orderBy(F.col("delivered_date").desc(), "id")
w5 = Window.partitionBy("status", "carrier").orderBy("cost", "id").rowsBetween(-5, Window.currentRow)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.min("amount").alias("min_payment_amount"))

scored = (
    df_shipments
    .withColumn("prev_cost", F.lag("cost", 1).over(w))
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("cost_avg_5", F.avg("cost").over(w5))
)

result = (
    scored
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "row_seq",
        "cost_avg_5",
        F.when(F.col("carrier").rlike("^(FedEx|Kerry)$"), "PRIORITY").when(F.col("carrier").rlike("^(DHL|Flash)$"), "GROUP_A").otherwise("CORE").alias("carrier_zone"),
        F.col("min_payment_amount"),
    )
)
