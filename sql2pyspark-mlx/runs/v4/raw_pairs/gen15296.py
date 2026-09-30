from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "cost",
        F.date_format("shipped_date", "MMM yyyy").alias("shipped_date_label"),
    )
)
