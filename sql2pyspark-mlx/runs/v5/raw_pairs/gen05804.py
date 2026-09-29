from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code")

exploded = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("code"))

valid = (
    exploded
    .filter(F.col("customer_id").isNotNull())
    .withColumn("code_lower", F.lower("code"))
)

base = (
    valid
    .filter(F.col("customer_id").isNotNull() & (F.col("code").eqNullSafe(F.lit("VIP"))))
    .withColumn("code_length", F.length("code"))
)

scored = (
    base
    .select(
        "code",
        "code_lower",
        F.max("code_length").over(w).alias("code_max_code_length"),
    )
)

metrics = (
    scored
    .filter(F.col("code") == "VIP")
    .select(
        "code",
        "code_lower",
        F.avg("code_max_code_length").over(w).alias("code_avg_code_max_code_length"),
        F.count("code_max_code_length").over(w).alias("code_count_code_max_code_length"),
    )
)

cleaned = (
    metrics
    .filter(F.col("code").isNotNull() & (~F.col("code").eqNullSafe(F.lit("VIP"))))
    .select(
        "*",
        F.when(F.col("code_count_code_max_code_length") >= 18, "High").when(F.col("code_count_code_max_code_length") >= 6, "Medium").otherwise("Low").alias("code_count_code_max_code_length_band"),
    )
)

result = (
    cleaned
    .select(
        "code",
        "code_lower",
        "code_avg_code_max_code_length",
        "code_count_code_max_code_length_band",
    )
    .orderBy(F.col("code").asc_nulls_first(), "code_avg_code_max_code_length")
)
