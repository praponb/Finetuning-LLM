from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "DELIVERED")
    .select(
        "id",
        "cost",
        "status",
        F.when(F.col("status").isin(["DELIVERED", "IN_TRANSIT"]), "Core").otherwise("Other").alias("status_group"),
        F.col("cost").cast("int").alias("cost_int"),
    )
    .orderBy(F.col("cost").desc())
)
