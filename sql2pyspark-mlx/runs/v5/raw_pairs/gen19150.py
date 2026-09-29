from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Singapore")
    .select("city", F.lit("supplier").alias("kind"))
    .unionByName(
        df_stores
        .filter(F.length("store_name") < 6)
        .select("city", F.lit("store").alias("kind"))
    )
)
