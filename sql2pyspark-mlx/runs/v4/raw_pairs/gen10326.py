from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_welcome5")
w2 = Window.orderBy(F.col("is_welcome5").desc_nulls_last())
w3 = Window.partitionBy("customer_id", "element").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("element")
w_running = Window.partitionBy("customer_id")

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

joined = (
    lines
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.email"), F.col("c.city"))
)

valid = (
    joined
    .filter(F.col("element") == "VIP")
    .select(
        "*",
        F.when(F.col("element") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
    )
)

with_windows = (
    valid
    .withColumn("is_welcome5_percent_rank", F.percent_rank().over(w))
    .withColumn("is_welcome5_cume_dist", F.cume_dist().over(w2))
    .withColumn("is_welcome5_max_1h", F.max("is_welcome5").over(w3))
    .withColumn("element_sum_is_welcome5", F.sum("is_welcome5").over(w4))
    .withColumn("customer_id_sum_is_welcome5", F.sum("is_welcome5").over(w_running))
)

result = (
    with_windows
    .withColumn("is_welcome5_percent_rank_tier", F.when(F.col("is_welcome5_percent_rank") >= 0.61, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["is_welcome5_percent_rank_tier", "city"], ["is_welcome5_percent_rank_tier"]],
        "is_welcome5_percent_rank_tier",
        "city",
    )
    .agg(
        F.round(F.avg("is_welcome5_cume_dist"), 2).alias("avg_is_welcome5_cume_dist"),
        F.sum(F.when(F.col("element") == "BUNDLE", F.col("element_sum_is_welcome5")).otherwise(0)).alias("bundle_element_sum_is_welcome5"),
        F.countDistinct("element").alias("distinct_elements"),
        F.countDistinct("city").alias("distinct_cities"),
        F.grouping_id().alias("gid"),
    )
)
