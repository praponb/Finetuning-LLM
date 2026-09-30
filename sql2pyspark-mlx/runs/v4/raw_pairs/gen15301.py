from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "device").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("amount").alias("sum_order_amount"))

enriched = (
    df_sessions
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w2))
)

result = (
    enriched
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "event_seq",
        "last_known_country",
        F.col("sum_order_amount"),
    )
    .orderBy(F.col("customer_id").desc())
)
