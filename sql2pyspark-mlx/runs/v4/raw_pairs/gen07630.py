from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code")
w2 = Window.orderBy(F.col("is_welcome5").desc_nulls_first())

exploded = df_carts.select("id", F.explode("coupon_codes").alias("code"))

cleaned = (
    exploded
    .filter(F.col("code") == "WELCOME5")
    .select(
        "*",
        F.substring("code", 1, 2).alias("code_prefix"),
        F.when(F.col("code") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.min("is_welcome5").over(w).alias("code_min_is_welcome5"),
        F.rank().over(w2).alias("is_welcome5_rank"),
    )
)

filtered = (
    enriched
    .filter(F.col("code") == "WELCOME5")
    .withColumn("code_title", F.initcap(F.trim("code")))
    .withColumn("code_clean", F.coalesce("code", F.lit("N/A")))
)

result = (
    filtered
    .select(
        "id",
        "code",
        "code_prefix",
        "is_welcome5",
        "code_min_is_welcome5",
        "is_welcome5_rank",
        "code_title",
        "code_clean",
    )
    .orderBy(F.col("is_welcome5_rank").desc_nulls_first())
)
