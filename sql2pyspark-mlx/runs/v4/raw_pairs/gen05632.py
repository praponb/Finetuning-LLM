from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "status",
        F.when(F.col("cost") >= 83, "High").when(F.col("cost") >= 39, "Medium").otherwise("Low").alias("cost_band"),
    )
)
