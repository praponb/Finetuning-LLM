from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date", F.lit("shipment").alias("source"))
    .unionByName(
        df_store_sales
        .select(
        F.col("sale_date").alias("shipped_date"),
        F.lit("store_sale").alias("source"),
    )
    )
    .distinct()
)
