from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.month("delivered_date").alias("delivered_month"))
    .agg(
        F.max("cost").alias("max_cost"),
        F.countDistinct("cost").alias("unique_statuss"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"),
    )
    .orderBy(F.col("unique_statuss").desc())
)
