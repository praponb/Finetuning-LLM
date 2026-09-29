from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
    .orderBy(F.col("unique_stores").desc())
)
