from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("order_id") == 12) & F.col("order_id").isin([12, 33, 35]))
)

summary = (
    base
    .groupBy("carrier")
    .agg(
        F.max("cost").alias("max_cost"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
    )
)

result = (
    summary
    .filter(F.col("max_cost") >= 43)
    .orderBy(F.col("returned_count").desc())
)
