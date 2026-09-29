from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "promo")
w2 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "entry"))
    .select(
        "id",
        "customer_id",
        "created_at",
        (F.col("pos") + 1).alias("line_no"),
        F.col("entry.product_id"),
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

result = (
    flattened
    .select(
        "*",
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
        F.max("product_id").over(w2).alias("product_id_max_30m"),
    )
)
