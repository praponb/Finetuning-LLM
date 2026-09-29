from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-05-01", "2023-09-01"))
    .select(
        "id",
        "delivered_date",
        "shipped_date",
        F.when(F.col("carrier").isin(["Flash", "Kerry"]), "Group A").otherwise("Other").alias("carrier_group"),
        F.dayofweek("delivered_date").alias("delivered_date_dayofweek"),
    )
)
