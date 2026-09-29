from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code")
w2 = Window.partitionBy("code").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("code").orderBy(F.col("product_id").asc_nulls_first())
w_running = Window.partitionBy("code").orderBy(F.col("product_id").desc_nulls_first())
w_part = Window.partitionBy("code").orderBy(F.col("code_length").desc_nulls_first())

flattened = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.price"),
    )
)

exploded = (
    flattened
    .select("id", "created_at", "product_id", F.explode("coupon_codes").alias("code"))
)

metrics = (
    exploded
    .filter(F.col("created_at") >= "2025-08-01")
    .withColumn("product_id_share", F.round((F.col("product_id") / F.sum("product_id").over(w)), 4))
    .withColumn("product_id_max_2h", F.max("product_id").over(w2))
    .withColumn("product_id_percent_rank", F.percent_rank().over(w3))
)

valid = (
    metrics
    .filter((F.col("created_at") >= F.lit("2025-03-02 02:00:00").cast("timestamp")) & F.col("code").isNotNull())
    .withColumn("code_length", F.length("code"))
    .withColumn("code_filled", F.coalesce("code", F.lit("N/A")))
)

metrics_carts = (
    valid
    .select(
        "created_at",
        "product_id",
        "product_id_max_2h",
        F.rank().over(w_running).alias("product_id_rank"),
        F.avg("product_id_max_2h").over(w).alias("code_avg_product_id_max_2h"),
        F.rank().over(w_part).alias("code_length_rank"),
    )
)

filtered = (
    metrics_carts
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-01"))) & (F.to_date("created_at") >= "2025-03-22"))
    .select(
        "created_at",
        "product_id",
        "product_id_max_2h",
        "product_id_rank",
        "code_avg_product_id_max_2h",
        "code_length_rank",
        F.to_date("created_at").alias("created_at_date"),
    )
)

result = (
    filtered
    .select(
        "product_id",
        "product_id_max_2h",
        "product_id_rank",
        "code_avg_product_id_max_2h",
        "code_length_rank",
        "created_at_date",
    )
)
