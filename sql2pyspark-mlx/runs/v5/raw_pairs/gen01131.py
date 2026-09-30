from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code")
w2 = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_last())
w3 = Window.partitionBy("code").orderBy(F.col("age_filled").desc_nulls_first())
w4 = Window.partitionBy("code").orderBy(F.col("code_sum_age").desc_nulls_last())
w5 = Window.partitionBy("customer_id")

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("code"))
)

enriched = (
    lines
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.city"), F.col("d.age"))
)

filtered = (
    enriched
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "code",
        "city",
        "age",
        F.col("created_at").cast("date").alias("created_at_day"),
        F.coalesce("age", F.lit(0)).alias("age_filled"),
    )
)

with_windows = (
    filtered
    .filter(F.col("age_filled").between(27, 46))
    .select(
        "id",
        "customer_id",
        "code",
        "age",
        "created_at_day",
        "age_filled",
        F.sum("age").over(w).alias("code_sum_age"),
        F.rank().over(w2).alias("age_rank"),
    )
)

scored = (
    with_windows
    .withColumn("age_filled_dense_rank", F.dense_rank().over(w3))
    .withColumn("code_sum_age_cume_dist", F.cume_dist().over(w4))
    .withColumn("customer_id_sum_code_sum_age", F.sum("code_sum_age").over(w5))
    .withColumn("code_max_age_rank", F.max("age_rank").over(w))
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "code",
        "age",
        "created_at_day",
        "age_filled",
        "age_rank",
        "age_filled_dense_rank",
        "customer_id_sum_code_sum_age",
        F.when(F.col("code").rlike("^(SPRING10|BUNDLE)$"), "EXTENDED").when(F.col("code").rlike("^(VIP|WELCOME5)$"), "GROUP_B").otherwise("PRIORITY").alias("code_zone"),
    )
    .orderBy(F.col("id").desc())
)
