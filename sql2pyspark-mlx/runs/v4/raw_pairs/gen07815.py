from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Kerry")
    .groupBy(
        F.when(F.col("status").isin(["RETURNED", "LOST"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(F.count("cost").alias("cost_count"))
    .filter(F.col("cost_count") >= 5)
    .select("status_group", "cost_count")
    .orderBy("cost_count")
)
