from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("items_total").asc_nulls_first())
w2 = Window.orderBy("items_total", "id").rowsBetween(-3, -1)

flattened_attrs = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        "ship_to",
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("none")).alias("utm_source_or_default"),
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    flattened_attrs
    .withColumn("items_total_dense_rank", F.dense_rank().over(w))
    .withColumn("items_total_avg_3_prior", F.avg("items_total").over(w2))
)
