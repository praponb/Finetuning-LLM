from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-02-05", "2025-10-01"))
    .groupBy(
        F.when(F.col("cost") >= 86, "high").when(F.col("cost") >= 29, "mid").otherwise("low").alias("cost_bucket"),
        F.year("shipped_date").alias("shipped_year"),
    )
    .agg(
        F.countDistinct("cost").alias("unique_carriers"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("unique_carriers") < 9)
    .select("cost_bucket", "shipped_year", "unique_carriers", "min_cost")
    .orderBy(F.col("min_cost").desc(), "shipped_year")
)
