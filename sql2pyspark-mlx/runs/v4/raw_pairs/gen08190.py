from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(6855, 11147) & F.col("opened_date").between("2024-10-01", "2025-07-01"))
    .select("region", F.coalesce("sqft", F.lit(0)).alias("sqft_filled"))
)
