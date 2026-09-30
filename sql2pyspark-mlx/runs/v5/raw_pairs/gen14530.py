from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("bulk_lines", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("bulk_lines", "id").rowsBetween(-6, Window.currentRow)

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.coalesce(F.col("attributes")["gift"], F.lit("direct")).alias("gift_or_default"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "customer_id",
        "created_at",
        "bulk_lines",
        "gift_or_default",
        F.count("bulk_lines").over(w).alias("last3_count_bulk_lines"),
        F.avg("bulk_lines").over(w).alias("last3_avg_bulk_lines"),
        F.max("bulk_lines").over(w).alias("last3_max_bulk_lines"),
        F.sum("bulk_lines").over(w).alias("last3_sum_bulk_lines"),
        F.avg("bulk_lines").over(w2).alias("bulk_lines_avg_6"),
    )
)
