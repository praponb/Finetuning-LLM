from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "EXPIRED", "ACTIVE"]))
    .select(F.lit("subscription").alias("source"), "monthly_fee")
    .union(
        df_store_sales
        .filter(F.col("sale_date").between("2024-08-01", "2025-10-18"))
        .select(F.lit("store_sale").alias("source"), "revenue")
    )
)
