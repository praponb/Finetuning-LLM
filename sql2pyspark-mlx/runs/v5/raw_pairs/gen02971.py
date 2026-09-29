from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("carrier") == "DHL") & (F.col("delivered_date") >= "2023-05-01"))
    .select(
        "shipped_date",
        "carrier",
        "order_id",
        F.col("cost").cast("int").alias("cost_int"),
        F.concat("status", F.lit(" "), "carrier").alias("status_carrier"),
    )
)
