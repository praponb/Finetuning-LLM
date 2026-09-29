from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.min("cost").alias("min_cost"),
        F.min("delivered_date").alias("first_delivered_date"),
    )
    .orderBy("first_delivered_date")
)
