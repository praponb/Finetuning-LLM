from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(F.col("city").isin(["Berlin", "London", "Tokyo"]) & (F.col("segment") == "SMB") & (F.col("city") != "New York"))
)

customers_stats = (
    recent_customers
    .groupBy(
        F.when(F.col("city").isin(["New York", "Berlin"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("age")).otherwise(0)).alias("jp_age"),
    )
)

result = customers_stats.orderBy("jp_age", "city_group")
