from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "Flash", "DHL"]) & (F.col("carrier") == "DHL"))
    .groupBy("order_id")
    .agg(
        F.max("cost").alias("max_cost"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("max_cost").desc())
)
