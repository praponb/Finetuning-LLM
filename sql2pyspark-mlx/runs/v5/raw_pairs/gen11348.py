from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(~F.col("city").isin(["Tokyo", "Bangkok"]) & (F.col("country") == "JP") & F.col("rating").between(3.7, 4.2))
)

agg = (
    recent_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min("rating").alias("min_rating"))
)

result = (
    agg
    .orderBy("rating_bucket")
    .limit(50)
)
