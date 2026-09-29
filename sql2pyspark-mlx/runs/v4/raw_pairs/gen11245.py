from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Sports", "Fashion"]) & ((F.col("price") <= 1050) | (F.col("brand") != "Acme")))
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", F.col("price")).otherwise(0)).alias("initech_price"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
    )
    .orderBy("initech_price")
)
