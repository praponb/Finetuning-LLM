from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") < "2025-01-19") & (F.col("cost") < 43))
    .select(
        "id",
        "shipped_date",
        "status",
        F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0).alias("is_in_transit"),
    )
)
