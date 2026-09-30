from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") < "2023-05-22") & F.col("status").isin(["LOST", "DELIVERED", "RETURNED"]))
    .groupBy("carrier")
    .agg(
        F.min("cost").alias("min_cost"),
        F.avg("cost").alias("avg_cost"),
    )
    .orderBy(F.col("avg_cost").desc())
)
