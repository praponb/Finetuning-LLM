from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w_part = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("started_at", "id")
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("amount").alias("avg_order_amount"))

enriched = (
    df_sessions
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
        F.row_number().over(w2).alias("event_seq"),
        F.first("browser", ignorenulls=True).over(w_part).alias("first_known_browser"),
        F.last("country", ignorenulls=True).over(w4).alias("last_known_country"),
        F.row_number().over(w5).alias("seq_no"),
    )
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
        "first_known_country",
        "event_seq",
        "first_known_browser",
        "last_known_country",
        F.col("avg_order_amount"),
    )
)
