from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("product_id").desc())
w2 = Window.partitionBy("promo")
w3 = Window.partitionBy("promo").orderBy(F.col("product_id").desc_nulls_last())

lines = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

result = (
    lines
    .select(
        "*",
        F.cume_dist().over(w).alias("product_id_cume_dist"),
        F.count("product_id").over(w2).alias("promo_count_product_id"),
        F.percent_rank().over(w3).alias("product_id_percent_rank"),
        F.round((F.col("product_id") / F.sum("product_id").over(w2)), 4).alias("product_id_share"),
    )
)
