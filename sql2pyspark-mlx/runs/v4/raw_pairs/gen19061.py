from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("weight_kg", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("weight_kg").desc(), "id")

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "attributes",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("unknown")).alias("utm_source_or_default"),
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.lead("weight_kg", 1, 0).over(w).alias("next_weight_kg"),
        F.ntile(3).over(w2).alias("weight_kg_bucket"),
    )
)
