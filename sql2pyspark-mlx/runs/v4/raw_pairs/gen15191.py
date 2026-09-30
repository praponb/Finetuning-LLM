from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city", F.lit("supplier").alias("source"))
    .unionByName(
        df_stores
        .filter(F.col("opened_date").between("2024-07-25", "2025-09-16"))
        .select("city", F.lit("store").alias("source"))
    )
    .orderBy("source")
)
