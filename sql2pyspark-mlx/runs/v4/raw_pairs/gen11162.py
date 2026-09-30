from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(~F.col("supplier_name").like("G%") & ~F.col("city").isin(["Tokyo", "Singapore"]))
)

suppliers_stats = (
    recent_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.max("rating").alias("max_rating"),
    )
)

result = (
    suppliers_stats
    .filter(F.col("avg_rating") > 46)
    .orderBy(F.col("rating_bucket").desc())
)
