from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Bangkok", "New York", "Tokyo"]))
    .select("city", F.round((F.col("rating") * 0.9), 2).alias("adjusted_rating"))
    .orderBy("city")
)
