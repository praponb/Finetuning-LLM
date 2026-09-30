from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "DE") & F.col("city").isin(["Tokyo", "Singapore", "Berlin"]))
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("num_suppliers"),
        F.max("rating").alias("max_rating"),
    )
    .filter(F.col("num_suppliers") < 3)
    .orderBy(F.col("max_rating").desc(), "country", "rating_bucket")
    .limit(20)
)
