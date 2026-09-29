from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy(F.col("items_total").desc(), "id")
w2 = Window.partitionBy("ship_city", "customer_id").orderBy("items_total", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("items_total", "id")

unpacked = (
    df_carts
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "ship_to",
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    unpacked
    .withColumn("items_total_bucket", F.ntile(10).over(w_grp))
    .withColumn("items_total_avg_3", F.avg("items_total").over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)
