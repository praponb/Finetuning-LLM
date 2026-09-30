from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("customer_id", "promo")
w4 = Window.orderBy(F.col("qty").desc_nulls_last())

exploded = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.count("qty").over(w).alias("qty_count_15m"),
        F.avg("qty").over(w2).alias("qty_avg_1h"),
        F.round((F.col("qty") / F.sum("qty").over(w3)), 4).alias("qty_share"),
        F.cume_dist().over(w4).alias("qty_cume_dist"),
    )
)
