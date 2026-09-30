from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("created_at", "id")
w3 = Window.orderBy(F.col("created_at").desc(), "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc(), "id")
w5 = Window.orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "ship_to",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.coalesce(F.col("attributes").getItem("device"), F.lit("direct")).alias("device_or_default"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("attributes")["gift"].alias("gift"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.first("utm_source", ignorenulls=True).over(w).alias("first_known_utm_source"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w4).alias("row_seq_2"),
        F.last("device_or_default", ignorenulls=True).over(w5).alias("last_known_device_or_default"),
    )
)
