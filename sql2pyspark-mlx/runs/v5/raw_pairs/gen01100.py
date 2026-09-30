from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.year("shipped_date").alias("shipped_year"))
    .agg(
        F.count("cost").alias("cost_count"),
        F.sum(F.when(F.col("carrier") == "FedEx", F.col("cost")).otherwise(0)).alias("fedex_cost"),
    )
    .filter(F.col("cost_count") < 6)
    .orderBy(F.col("fedex_cost").desc())
)
