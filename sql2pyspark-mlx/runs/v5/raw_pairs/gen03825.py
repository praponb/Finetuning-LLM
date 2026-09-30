from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("num_items").desc(), "id")
w_part = Window.partitionBy("ship_country").orderBy("num_items", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("ship_country").orderBy(F.col("num_items").asc_nulls_last())

parsed = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.size("items").alias("num_items"),
        F.col("ship_to.country").alias("ship_country"),
        F.col("attributes")["device"].alias("device"),
        F.col("attributes")["gift"].alias("gift"),
    )
)

result = (
    parsed
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .select(
        "*",
        F.ntile(4).over(w).alias("num_items_bucket"),
        F.first("device", ignorenulls=True).over(w_part).alias("first_known_device"),
        F.dense_rank().over(w3).alias("num_items_dense_rank"),
    )
)
