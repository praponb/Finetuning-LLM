from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") != "Berlin") & ~F.col("city").isin(["New York", "Berlin", "Bangkok"]) & (F.col("region") == "East"))
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "South", 1).otherwise(0))) / F.count("*")), 1).alias("pct_south"),
        F.round(((100 * F.sum(F.when(F.col("region") == "West", 1).otherwise(0))) / F.count("*")), 1).alias("pct_west"),
        F.sum(F.when(F.col("region") == "South", 1).otherwise(0)).alias("south_count"),
    )
    .orderBy("city")
    .limit(50)
)
