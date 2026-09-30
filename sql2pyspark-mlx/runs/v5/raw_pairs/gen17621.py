from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w_grp = Window.orderBy(F.col("created_at_quarter").desc_nulls_last())
w3 = Window.orderBy(F.col("age").desc_nulls_last())
w4 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy(F.col("created_at_quarter_minus_is_spring10").asc_nulls_last())
w6 = Window.partitionBy("customer_id").orderBy(F.col("customer_id_min_created_at_quarter").desc())
w7 = Window.orderBy("created_at_quarter")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

filtered = (
    flattened
    .filter(F.col("created_at") >= F.lit("2025-03-02 01:00:00").cast("timestamp"))
    .withColumn("created_at_quarter", F.quarter("created_at"))
    .withColumn("is_spring10", F.when(F.col("element") == "SPRING10", 1).otherwise(0))
)

with_dims = (
    filtered
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .filter(~F.col("d.is_active"))
    .select(
        "e.id",
        "e.customer_id",
        "e.created_at",
        "e.created_at_quarter",
        "e.is_spring10",
        F.col("d.age"),
        F.col("d.signup_date"),
        F.col("d.credit_limit"),
    )
)

cleaned = (
    with_dims
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        (F.col("created_at_quarter") - F.col("is_spring10")).alias("created_at_quarter_minus_is_spring10"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.min("created_at_quarter").over(w).alias("customer_id_min_created_at_quarter"),
        F.cume_dist().over(w_grp).alias("created_at_quarter_cume_dist"),
        F.dense_rank().over(w3).alias("age_dense_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "customer_id",
        "created_at",
        "age",
        "created_at_quarter_minus_is_spring10",
        "customer_id_min_created_at_quarter",
        "age_dense_rank",
        F.max("created_at_quarter").over(w4).alias("created_at_quarter_max_2h"),
        F.stddev_samp("age_dense_rank").over(w).alias("customer_id_stddev_samp_age_dense_rank"),
        F.cume_dist().over(w5).alias("created_at_quarter_minus_is_spring10_cume_dist"),
        F.rank().over(w6).alias("customer_id_min_created_at_quarter_rank"),
        F.dense_rank().over(w7).alias("created_at_quarter_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "created_at",
        "age",
        "customer_id_min_created_at_quarter",
        "created_at_quarter_max_2h",
        "created_at_quarter_minus_is_spring10_cume_dist",
        "customer_id_min_created_at_quarter_rank",
        "created_at_quarter_dense_rank",
    )
    .orderBy(F.col("created_at_quarter_max_2h").asc_nulls_first())
)
