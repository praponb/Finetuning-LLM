from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["DE", "SG"]))
    .select("city", F.lit("supplier").alias("source"))
    .unionByName(
        df_stores
        .filter(F.col("sqft") == 9795)
        .select("city", F.lit("store").alias("source"))
    )
    .orderBy("source")
)
