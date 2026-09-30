from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("status").isin(["RETURNED", "LOST"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost")).otherwise(0)).alias("delivered_cost"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("delivered_cost") < 47)
    .select("status_group", "delivered_cost", "max_cost")
    .orderBy(F.col("delivered_cost").desc())
)
