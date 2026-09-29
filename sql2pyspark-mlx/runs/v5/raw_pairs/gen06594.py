from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(5719, 6341))
    .select(F.lit("store").alias("record_type"), "opened_date")
    .union(
        df_shipments
        .select(F.lit("shipment").alias("record_type"), "shipped_date")
    )
    .orderBy("record_type")
)
