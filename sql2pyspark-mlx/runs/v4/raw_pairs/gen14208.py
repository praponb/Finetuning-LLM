from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        F.min("shipped_date").alias("first_shipped_date"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_ups"),
    )
)
