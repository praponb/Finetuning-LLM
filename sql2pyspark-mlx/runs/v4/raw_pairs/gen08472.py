from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "status",
        "carrier",
        F.when(F.col("carrier") == "UPS", 1).otherwise(0).alias("is_ups"),
    )
    .orderBy("status")
)
