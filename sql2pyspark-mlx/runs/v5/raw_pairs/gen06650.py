from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") <= 96) & (F.col("status") == "RETURNED") & (F.col("shipped_date") >= "2023-12-03"))
    .select(
        "id",
        "status",
        "delivered_date",
        F.when(F.col("status") == "LOST", 1).otherwise(0).alias("is_lost"),
        F.round((F.col("cost") * 0.85), 2).alias("cost_adjusted"),
    )
)
