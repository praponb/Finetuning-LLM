from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc(), "id")

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.coalesce(F.col("attributes")["device"], F.lit("unknown")).alias("device_or_default"),
        F.col("ship_to.country").alias("ship_country"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "customer_id",
        "device_or_default",
        "ship_country",
        F.row_number().over(w).alias("event_seq"),
    )
)
