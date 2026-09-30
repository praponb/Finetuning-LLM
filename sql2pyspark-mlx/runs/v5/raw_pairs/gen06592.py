from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Bangkok", "Berlin", "Tokyo"]) & F.col("is_active"))
    .select("segment", "country", F.round("age", 2).alias("age_rounded"))
    .orderBy("segment")
)
