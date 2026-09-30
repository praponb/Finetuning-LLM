from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & F.col("carrier").isin(["UPS", "Flash"]))
    .select(
        "id",
        "cost",
        F.length("status").alias("status_length"),
        F.floor("cost").alias("cost_whole"),
    )
    .orderBy(F.col("cost").desc(), "id")
    .limit(50)
)
