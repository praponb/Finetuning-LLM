from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(-5, Window.currentRow)

flattened_attrs = (
    df_carts
    .filter(F.hour("created_at").between(9, 12))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
    )
)

result = (
    flattened_attrs
    .withColumn("last_known_weight_kg", F.last("weight_kg", ignorenulls=True).over(w))
    .withColumn("weight_kg_avg_1h", F.avg("weight_kg").over(w2))
    .withColumn("weight_kg_avg_5", F.avg("weight_kg").over(w3))
)
