from pyspark.sql import functions as F

src = df_shipments.filter(F.col("carrier").isin(["UPS", "Flash"]))

agg = (
    src
    .groupBy("order_id")
    .agg(
        F.min("delivered_date").alias("min_delivered_date"),
        F.count("cost").alias("cost_count"),
    )
)

result = (
    agg
    .filter(F.col("cost_count") < 5)
    .select("order_id", "cost_count")
    .orderBy("cost_count")
)
