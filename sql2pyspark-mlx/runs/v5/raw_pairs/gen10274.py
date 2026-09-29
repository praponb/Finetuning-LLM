from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 16253, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft"))).alias("bangkok_total_sqft"),
        F.sum("sqft").alias("total_sqft"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
