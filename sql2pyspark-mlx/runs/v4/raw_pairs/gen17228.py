from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Kerry")
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_in_transit"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("max_cost") < 51)
    .orderBy(F.col("pct_in_transit").desc())
)
