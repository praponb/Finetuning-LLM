from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "delivered_date",
        "shipped_date",
        F.initcap(F.trim("status")).alias("status_title"),
        F.date_format("shipped_date", "yyyy-MM-dd").alias("shipped_date_label"),
    )
)
