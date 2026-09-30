from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("carrier") == "Kerry")
    .withColumn("clean_carrier", F.trim("carrier"))
)

agg = (
    recent_shipments
    .groupBy("status")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.round(((100 * F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_delivered"),
    )
)

result = agg.orderBy(F.col("pct_delivered").desc())
