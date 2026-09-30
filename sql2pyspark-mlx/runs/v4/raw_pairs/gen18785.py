from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("ship_country").orderBy("num_items", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("num_attributes", "id").rowsBetween(-2, -1)

flattened_attrs = (
    df_carts
    .filter(F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "id",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.size("attributes").alias("num_attributes"),
        F.col("attributes")["gift"].alias("gift"),
        F.size("items").alias("num_items"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
        F.col("ship_to.country").alias("ship_country"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.sum("num_attributes").over(w).alias("running_num_attributes"),
        F.avg("num_attributes").over(w2).alias("num_attributes_avg_2_prior"),
    )
)
