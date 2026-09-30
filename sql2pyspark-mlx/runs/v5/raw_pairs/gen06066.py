from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "ship_city").orderBy("bulk_lines", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_carts
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "attributes",
        F.size(F.filter("items", lambda li: li["qty"] >= 3)).alias("bulk_lines"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.size("attributes").alias("num_attributes"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("unknown")).alias("utm_source_or_default"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    parsed
    .select(
        "id",
        "created_at",
        "bulk_lines",
        "has_welcome_code",
        "num_attributes",
        "ship_city",
        F.min("bulk_lines").over(w).alias("running_min_bulk_lines"),
        F.max("bulk_lines").over(w).alias("running_max_bulk_lines"),
        F.avg("bulk_lines").over(w).alias("running_avg_bulk_lines"),
    )
)
