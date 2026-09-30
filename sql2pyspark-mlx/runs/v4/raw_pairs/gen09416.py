from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")

valid = (
    df_promotions
    .filter(F.col("ends_on").between("2023-03-31", "2024-01-01"))
    .withColumn("channel_length", F.length("channel"))
    .withColumn("channel_promo_code", F.concat("channel", F.lit(", "), "promo_code"))
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered = (
    latest
    .filter(F.col("channel_length") > 20)
    .select(
        "*",
        F.when(F.col("promo_code") == "SPRING10", 1).otherwise(0).alias("is_spring10"),
    )
)

result = (
    filtered
    .select("id", "promo_code", "discount_pct", "channel_length", "channel_promo_code")
    .orderBy(F.col("id").desc_nulls_last(), F.col("channel_length").desc())
)
