from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") >= 2025)
    .select("status", "carrier", F.round((F.col("cost") * 0.9), 2).alias("cost_scaled"))
)
