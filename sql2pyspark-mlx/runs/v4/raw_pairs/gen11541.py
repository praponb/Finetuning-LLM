from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("product_id").asc_nulls_last())
w2 = Window.partitionBy("customer_id")

exploded = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
    )
)

result = (
    exploded
    .withColumn("product_id_rank", F.rank().over(w))
    .withColumn("customer_id_avg_product_id", F.avg("product_id").over(w2))
)
