from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("value")
w3 = Window.partitionBy("value").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("value").orderBy("pos")
w5 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w7 = Window.partitionBy("value").orderBy(F.col("pos_avg_30m").asc_nulls_first())
w8 = Window.partitionBy("value").orderBy(F.col("pos_max_15m").asc_nulls_first())
w9 = Window.partitionBy("value").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "li_item"))
    .select("id", "created_at", "coupon_codes", "pos", F.col("li_item.price"))
)

lines = (
    flattened
    .select("id", "created_at", "pos", F.explode("coupon_codes").alias("value"))
)

with_windows = (
    lines
    .withColumn("pos_avg_30m", F.avg("pos").over(w))
    .withColumn("value_count_pos", F.count("pos").over(w2))
    .withColumn("pos_max_15m", F.max("pos").over(w3))
    .withColumn("pos_cume_dist", F.cume_dist().over(w4))
    .withColumn("pos_max_30m", F.max("pos").over(w5))
)

metrics = (
    with_windows
    .select(
        "*",
        F.sum("pos_max_30m").over(w6).alias("pos_max_30m_sum_1h"),
        F.count("pos").over(w2).alias("value_count_pos_2"),
        F.dense_rank().over(w7).alias("pos_avg_30m_dense_rank"),
        F.cume_dist().over(w8).alias("pos_max_15m_cume_dist"),
        F.count("pos_max_30m").over(w9).alias("pos_max_30m_count_2h"),
    )
)

result = (
    metrics
    .select(
        "id",
        "created_at",
        "pos",
        "value",
        "value_count_pos",
        "pos_cume_dist",
        "pos_max_30m",
        "pos_max_30m_sum_1h",
        "pos_max_15m_cume_dist",
        F.when(F.col("value").rlike("^(SPRING10|WELCOME5)$"), "EXTENDED").when(F.col("value").rlike("^(VIP|BUNDLE)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("value_zone"),
    )
    .orderBy(F.col("pos_max_15m_cume_dist").desc_nulls_first())
)
