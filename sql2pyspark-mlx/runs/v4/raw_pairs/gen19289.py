from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("max_item_price", "id")
w2 = Window.orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("weight_kg").desc_nulls_last())

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.coalesce(F.col("attributes")["device"], F.lit("direct")).alias("device_or_default"),
    )
)

result = (
    flattened_attrs
    .withColumn("next_max_item_price", F.lead("max_item_price", 2).over(w))
    .withColumn("last_known_weight_kg", F.last("weight_kg", ignorenulls=True).over(w2))
    .withColumn("weight_kg_rank", F.rank().over(w3))
)
