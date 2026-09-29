from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("max_item_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(-3, Window.currentRow)

parsed = (
    df_carts
    .filter(F.col("created_at") < "2024-02-01")
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
        F.coalesce(F.col("attributes").getItem("device"), F.lit("none")).alias("device_or_default"),
        F.col("ship_to.zip").alias("ship_zip"),
    )
)

result = (
    parsed
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("running_max_item_price", F.sum("max_item_price").over(w2))
    .withColumn("customer_id_min_max_item_price", F.min("max_item_price").over(w3))
    .withColumn("max_item_price_avg_3", F.avg("max_item_price").over(w4))
)
