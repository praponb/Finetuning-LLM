from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_carts
    .filter(F.col("created_at") < "2024-07-01")
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        "attributes",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.transform("items", lambda li: li["product_id"]).alias("product_ids"),
        F.coalesce(F.col("attributes")["device"], F.lit("none")).alias("device_or_default"),
    )
)

result = (
    parsed
    .select(
        "id",
        "customer_id",
        "device_or_default",
        F.last("device_or_default", ignorenulls=True).over(w).alias("last_known_device_or_default"),
    )
)
