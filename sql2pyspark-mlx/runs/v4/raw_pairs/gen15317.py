from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-09-19").cast("date")) & F.col("order_id").isNotNull() & (F.col("status") != "DELIVERED"))
    .select(
        "*",
        F.length("carrier").alias("carrier_length"),
        F.when(F.col("status").isin(["DELIVERED", "LOST"]), "Core").otherwise("Other").alias("status_group"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.amount"))
)
