from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "FedEx"]))
    .groupBy("carrier")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("avg_cost") > 54)
    .select("carrier", "avg_cost", "max_cost")
    .orderBy(F.col("avg_cost").desc())
)
