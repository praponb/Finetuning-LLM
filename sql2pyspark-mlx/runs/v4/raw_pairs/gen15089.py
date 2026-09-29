from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "cost",
        F.when(F.col("cost") >= 100, "High").when(F.col("cost") >= 38, "Medium").otherwise("Low").alias("cost_tier"),
    )
)
