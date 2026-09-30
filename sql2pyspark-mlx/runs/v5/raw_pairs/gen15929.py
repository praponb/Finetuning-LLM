from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("num_items", "id")
w2 = Window.orderBy("num_items", "id").rowsBetween(-5, Window.currentRow)

unpacked = (
    df_carts
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "attributes",
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.size("items").alias("num_items"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.lead("num_items").over(w).alias("next_num_items"),
        F.row_number().over(w).alias("row_seq"),
        F.avg("num_items").over(w2).alias("num_items_avg_5"),
    )
)
