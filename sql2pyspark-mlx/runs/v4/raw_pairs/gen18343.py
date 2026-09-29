from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "TH", "UK"]))
    .select("city", F.lit("supplier").alias("source"))
    .unionByName(
        df_stores
        .filter(F.col("store_name").isNotNull())
        .select("city", F.lit("store").alias("source"))
    )
)
