from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id", "pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("pos", "id", "pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc(), "id", "pos")
w4 = Window.orderBy("created_at", "id", "pos").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("promo").orderBy(F.col("pos").desc(), "id", "pos")

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "entry"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

result = (
    lines
    .select(
        "id",
        "customer_id",
        "pos",
        "qty",
        "price",
        F.sum("qty").over(w).alias("cumulative_qty"),
        F.sum("qty").over(w2).alias("cumulative_qty_2"),
        F.row_number().over(w3).alias("row_seq"),
        F.avg("qty").over(w4).alias("qty_avg_3"),
        F.ntile(5).over(w5).alias("pos_bucket"),
    )
)
