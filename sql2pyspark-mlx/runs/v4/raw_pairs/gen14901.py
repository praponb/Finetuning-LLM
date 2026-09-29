from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_day").orderBy(F.col("spring10_count").desc())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

result = (
    exploded
    .groupBy(
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        "customer_id",
        "code",
    )
    .agg(
        F.size(F.collect_set("code")).alias("num_codes"),
        F.max("created_at").alias("ended_at"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("code") == "SPRING10", 1)).alias("spring10_count"),
        F.min("created_at").alias("first_seen"),
        F.sort_array(F.collect_set("code")).alias("code_list"),
    )
    .withColumn("spring10_count_rank", F.rank().over(w))
    .orderBy(F.col("spring10_count").desc_nulls_first())
)
