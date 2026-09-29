from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.length("supplier_name") >= 6) & F.col("city").isNull() & ~F.col("country").isin(["DE", "JP", "TH"]))
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum("rating").alias("total_rating"))
    .orderBy("rating_bucket")
    .limit(20)
)
