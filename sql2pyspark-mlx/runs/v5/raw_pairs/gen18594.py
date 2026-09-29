from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id", F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(
        F.max("cost").alias("max_cost"),
        F.min("cost").alias("min_cost"),
        F.min("shipped_date").alias("first_shipped_date"),
    )
    .orderBy(F.col("min_cost").desc())
)
