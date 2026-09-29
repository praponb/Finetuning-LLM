from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.count("cost").alias("cost_count"),
        F.min("delivered_date").alias("first_delivered_date"),
    )
    .filter(F.col("cost_count") > 5)
    .select("carrier", "cost_count", "first_delivered_date")
    .orderBy(F.col("first_delivered_date").desc(), "carrier")
)
