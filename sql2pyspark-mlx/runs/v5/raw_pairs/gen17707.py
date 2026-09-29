from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy("price", "id", "pos")
w2 = Window.partitionBy("promo").orderBy(F.col("qty").desc(), "id", "pos")

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    lines
    .select(
        "id",
        "pos",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
