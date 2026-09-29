from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("num_items", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("num_items").desc_nulls_last())

parsed = (
    df_carts
    .filter(F.hour("created_at").between(11, 18))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
        F.size("items").alias("num_items"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
        F.size("attributes").alias("num_attributes"),
    )
)

result = (
    parsed
    .withColumn("last_known_num_items", F.last("num_items", ignorenulls=True).over(w))
    .withColumn("num_items_dense_rank", F.dense_rank().over(w2))
)
