from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        "ship_to",
        "attributes",
        F.coalesce(F.col("attributes").getItem("device"), F.lit("direct")).alias("device_or_default"),
        F.col("ship_to.zip").alias("ship_zip"),
    )
)

result = (
    parsed
    .groupBy("customer_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.countDistinct("device_or_default").alias("distinct_device_or_defaults"),
    )
)
