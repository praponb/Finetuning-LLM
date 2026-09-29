from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") > 21)
    .select(
        F.lit("store_sale").alias("record_type"),
        F.col("sale_date").alias("event_date"),
    )
    .unionByName(
        df_shipments
        .filter(F.col("carrier").isNotNull())
        .select(
        F.lit("shipment").alias("record_type"),
        F.col("shipped_date").alias("event_date"),
    )
    )
    .distinct()
)
