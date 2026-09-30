from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("code").orderBy(F.col("age_count_2h").asc_nulls_last())

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "value"),
    )
)

flattened = (
    lines
    .select(
        "id",
        "customer_id",
        "created_at",
        "k",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

filtered = (
    flattened
    .filter(F.col("created_at") >= F.to_date(F.lit("2025-03-01")))
    .select(
        "id",
        "customer_id",
        "created_at",
        "k",
        "code",
        F.lower("code").alias("code_lower"),
        F.coalesce("code", F.lit("OTHER")).alias("code_clean"),
    )
)

with_dims = (
    filtered
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select("e.*", F.col("c.age"))
)

metrics = (
    with_dims
    .filter(F.col("created_at") >= "2023-07-01")
    .select("*", F.count("age").over(w).alias("age_count_2h"))
)

scored = (
    metrics
    .select(
        "id",
        "k",
        "code",
        "code_lower",
        "age",
        "age_count_2h",
        F.max("age").over(w2).alias("age_max_2h"),
        F.dense_rank().over(w3).alias("age_count_2h_dense_rank"),
    )
)

result = (
    scored
    .groupBy("k")
    .pivot("code", ["BUNDLE", "VIP"])
    .agg(
        F.sum("age_count_2h_dense_rank").alias("total"),
        F.count(F.lit(1)).alias("cnt"),
    )
)
