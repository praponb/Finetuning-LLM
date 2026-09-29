from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "cost",
        "order_id",
        "shipped_date",
        F.date_format("shipped_date", "yyyy-MM").alias("shipped_date_label"),
        F.date_format("shipped_date", "MMM yyyy").alias("shipped_date_label_2"),
    )
)
