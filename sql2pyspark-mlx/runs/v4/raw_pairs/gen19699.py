from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.countDistinct("cost").alias("unique_carriers"),
        F.min("cost").alias("min_cost"),
    )
    .orderBy(F.col("min_cost").desc(), F.col("status").desc())
)
