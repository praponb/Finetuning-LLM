from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("product_id").asc_nulls_first(), "id", "pos")
w2 = Window.partitionBy("customer_id").orderBy(F.col("cnt").desc())

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "pos",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.promo"),
    )
)

joined = (
    lines
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.segment"), F.col("d.country"), F.col("d.city"))
)

ranked = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy("customer_id", "product_id")
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("segment")).alias("segment_list_2"),
    )
    .withColumn("cnt_dense_rank", F.dense_rank().over(w2))
    .orderBy(F.col("cnt").desc())
)
