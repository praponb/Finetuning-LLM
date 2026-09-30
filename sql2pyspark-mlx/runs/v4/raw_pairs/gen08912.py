from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id")
w2 = Window.partitionBy("promo")
w3 = Window.partitionBy("customer_id").orderBy("price", "id", "pos")

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "pos",
        F.col("line.product_id"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    flattened
    .select(
        "*",
        F.max("pos").over(w_recent).alias("customer_id_max_pos"),
        F.min("product_id").over(w2).alias("promo_min_product_id"),
        F.row_number().over(w3).alias("event_seq"),
    )
)
