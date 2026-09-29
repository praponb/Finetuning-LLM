from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.col("delivered_date") <= "2025-01-16") & F.col("status").isin(["DELIVERED", "RETURNED"])) | (F.col("status") == "DELIVERED"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_in_transit"),
    )
    .orderBy(F.col("pct_in_transit").desc())
)
