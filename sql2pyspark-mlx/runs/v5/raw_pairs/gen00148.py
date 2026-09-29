from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("bulk_lines", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.orderBy("created_at", "id")

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        F.col("attributes")["device"].alias("device"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
        F.col("attributes")["gift"].alias("gift"),
        F.size(F.filter("items", lambda i: i["qty"] >= 3)).alias("bulk_lines"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "customer_id",
        "has_premium_item",
        "gift",
        "bulk_lines",
        F.lead("bulk_lines", 1).over(w).alias("next_bulk_lines"),
        F.sum("bulk_lines").over(w2).alias("bulk_lines_sum_30m"),
        F.row_number().over(w3).alias("event_seq"),
    )
)
