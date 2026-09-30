from pyspark.sql import functions as F

src = (
    df_customers
    .filter(F.col("segment").isin(["Corporate", "SMB"]) & (F.col("country") == "UK") & (F.col("city") != "Berlin"))
)

country_summary = (
    src
    .groupBy("country")
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.round(((100 * F.sum(F.when(F.col("country") == "DE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_de"),
    )
)

result = (
    country_summary
    .select("country", "pct_de")
    .orderBy("country")
)
