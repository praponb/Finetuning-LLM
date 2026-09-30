from pyspark.sql import functions as F

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.supplier_id").isin([10, 16, 27, 32])), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("rating") >= 4.3, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum(F.when(F.col("country") == "JP", F.col("rating"))).alias("jp_total_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating_2"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
    )
    .filter(F.col("uk_rows") < 1000)
    .orderBy(F.col("avg_rating").desc_nulls_first())
)
