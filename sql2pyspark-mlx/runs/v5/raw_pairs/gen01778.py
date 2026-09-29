from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("customer_id")

lines = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    lines
    .filter(F.col("promo") != "VIP")
    .select(
        "id",
        "created_at",
        "product_id",
        F.count("qty").over(w_hist).alias("qty_count_15m"),
        F.min("qty").over(w2).alias("customer_id_min_qty"),
    )
)
