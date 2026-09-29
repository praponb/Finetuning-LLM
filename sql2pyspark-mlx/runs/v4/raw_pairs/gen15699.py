from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("num_attributes").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("num_attributes", "id")
w4 = Window.partitionBy("customer_id").orderBy("created_at", "id")

unpacked = (
    df_carts
    .filter(F.to_date("created_at") < "2023-10-10")
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "attributes",
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.size("attributes").alias("num_attributes"),
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.coalesce(F.col("attributes")["device"], F.lit("unknown")).alias("device_or_default"),
    )
)

result = (
    unpacked
    .withColumn("num_attributes_sum_30m", F.sum("num_attributes").over(w))
    .withColumn("num_attributes_bucket", F.ntile(4).over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("next_cart_value", F.lead("cart_value", 2).over(w4))
)
