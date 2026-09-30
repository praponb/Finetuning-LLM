from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id") == 31) & (F.col("order_id").isNotNull() | F.col("delivered_date").between("2023-07-01", "2024-09-01")))
    .select(
        "id",
        "shipped_date",
        "cost",
        "order_id",
        F.when(F.col("carrier").isin(["Kerry", "FedEx"]), "Priority").otherwise("Other").alias("carrier_group"),
    )
    .orderBy(F.col("order_id").desc())
)
