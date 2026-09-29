from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_carts
    .filter(F.hour("created_at").between(11, 17))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("ship_to.city").alias("ship_city"),
        F.col("ship_to.country").alias("ship_country"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("direct")).alias("utm_source_or_default"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "utm_source_or_default",
        F.last("ship_city", ignorenulls=True).over(w).alias("last_known_ship_city"),
    )
)
