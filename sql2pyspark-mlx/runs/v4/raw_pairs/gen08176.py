from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("ship_city")
w2 = Window.orderBy("num_attributes", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("bulk_lines", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("ship_city").orderBy("num_attributes", "id")
w5 = Window.partitionBy("ship_city").orderBy("bulk_lines", "id")

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.size("attributes").alias("num_attributes"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.max("num_attributes").over(w).alias("ship_city_max_num_attributes"),
        F.avg("num_attributes").over(w2).alias("num_attributes_avg_6"),
        F.avg("bulk_lines").over(w3).alias("bulk_lines_avg_2_prior"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("seq_no"),
    )
)
