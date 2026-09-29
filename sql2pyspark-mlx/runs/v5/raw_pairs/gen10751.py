from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") != "Acme")
    .select(F.col("launch_date").alias("event_date"))
    .unionByName(
        df_shipments
        .filter(F.col("carrier").isin(["Kerry", "Flash", "FedEx"]))
        .select(F.col("delivered_date").alias("event_date"))
    )
    .orderBy("event_date")
)
