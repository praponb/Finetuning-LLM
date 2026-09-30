from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(55, 58))
    .select("delivered_date", F.lit("shipment").alias("origin"))
    .union(
        df_stores
        .filter(F.col("opened_date") >= "2023-04-03")
        .select("opened_date", F.lit("store").alias("origin"))
    )
    .distinct()
)
