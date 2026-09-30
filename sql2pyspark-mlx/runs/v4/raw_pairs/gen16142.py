from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "cost",
        F.date_format("shipped_date", "yyyy-MM").alias("shipped_date_label"),
    )
)
