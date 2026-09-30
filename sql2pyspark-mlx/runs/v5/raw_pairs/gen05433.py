from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("bulk_lines", "id")

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.coalesce(F.col("attributes")["utm_source"], F.lit("direct")).alias("utm_source_or_default"),
        F.size(F.filter("items", lambda li: li["qty"] >= 3)).alias("bulk_lines"),
        F.size("items").alias("num_items"),
    )
)

result = (
    parsed
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.ntile(3).over(w).alias("bulk_lines_bucket"),
        F.lead("bulk_lines", 1).over(w).alias("next_bulk_lines"),
    )
)
