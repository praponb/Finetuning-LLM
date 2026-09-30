from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "promo")
w2 = Window.orderBy(F.col("qty").asc_nulls_last())
w3 = Window.partitionBy("customer_id").orderBy(F.col("qty").desc_nulls_last())
w_running = Window.orderBy(F.col("qty").desc())

exploded = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select("id", "customer_id", F.col("line.qty"), F.col("line.promo"))
)

result = (
    exploded
    .select(
        "*",
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
        F.percent_rank().over(w2).alias("qty_percent_rank"),
        F.percent_rank().over(w3).alias("qty_percent_rank_2"),
        F.dense_rank().over(w_running).alias("qty_dense_rank"),
    )
)
