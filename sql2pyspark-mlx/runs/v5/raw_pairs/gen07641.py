from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "items",
        "attributes",
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.col("attributes").getItem("device").alias("device"),
        F.coalesce(F.col("attributes")["device"], F.lit("direct")).alias("device_or_default"),
    )
)
