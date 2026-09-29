from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("ship_country").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("ship_country").orderBy("created_at", "id")
w3 = Window.partitionBy("ship_country").orderBy(F.col("created_at").desc(), "id")

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        F.col("ship_to.country").alias("ship_country"),
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)

result = (
    flattened_attrs
    .withColumn("first_known_ship_country", F.first("ship_country", ignorenulls=True).over(w))
    .withColumn("first_known_ship_country_2", F.first("ship_country", ignorenulls=True).over(w))
    .withColumn("first_known_ship_country_3", F.first("ship_country", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)
