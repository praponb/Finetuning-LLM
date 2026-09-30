from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["North", "South"]) & (F.col("sqft") == 11486) & F.col("store_name").like("Air%"))
    .groupBy(
        F.when(F.col("city").isin(["Tokyo", "Bangkok"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(F.avg("sqft").alias("avg_sqft"))
    .filter(F.col("avg_sqft") > 4800)
)
