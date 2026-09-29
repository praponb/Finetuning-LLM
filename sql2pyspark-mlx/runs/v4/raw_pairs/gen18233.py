from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("code"))

base = (
    flattened
    .filter(F.col("code").isNotNull())
    .withColumn("code_length", F.length("code"))
    .withColumn("created_at_label", F.date_format("created_at", "MMM yyyy"))
    .withColumn("code_filled", F.coalesce("code", F.lit("NONE")))
)

base_carts = (
    base
    .filter((F.col("created_at") >= F.lit("2025-03-01 19:00:00").cast("timestamp")) & F.col("code").isNotNull())
    .select(
        "id",
        "created_at",
        "code_length",
        "created_at_label",
        "code_filled",
        F.coalesce("code_length", F.lit(0)).alias("code_length_filled"),
        F.ceil("code_length").alias("code_length_whole"),
        F.coalesce("code", F.lit("N/A")).alias("code_filled_2"),
    )
)

metrics = (
    base_carts
    .withColumn("code_length_filled_max_15m", F.max("code_length_filled").over(w))
    .withColumn("code_length_filled_count_1h", F.count("code_length_filled").over(w2))
)

result = (
    metrics
    .select(
        "id",
        "created_at_label",
        "code_filled",
        "code_length_filled",
        "code_length_whole",
        "code_filled_2",
        "code_length_filled_max_15m",
        "code_length_filled_count_1h",
    )
    .orderBy(F.col("code_length_filled_count_1h").asc_nulls_first())
)
