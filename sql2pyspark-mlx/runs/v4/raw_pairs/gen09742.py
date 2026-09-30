from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~((F.col("order_id") == 10) & ((F.col("shipped_date") >= "2023-08-08") | (F.col("order_id") == 38))))
    .select(
        "order_id",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.quarter("shipped_date").alias("shipped_date_quarter"),
    )
    .orderBy(F.col("shipped_date_quarter").desc(), "delivered_date_month_end")
)
