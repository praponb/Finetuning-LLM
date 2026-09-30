from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("promo")

lines = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.qty"),
        F.col("line.promo"),
    )
)

result = (
    lines
    .select(
        "id",
        "created_at",
        "promo",
        F.sum("qty").over(w).alias("qty_sum_15m"),
        F.max("qty").over(w2).alias("promo_max_qty"),
    )
)
