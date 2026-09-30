from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.max("cost").alias("max_cost"),
        F.count("cost").alias("cost_count"),
    )
)
