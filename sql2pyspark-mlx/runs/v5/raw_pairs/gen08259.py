from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id")

filtered = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-10-24").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.when(F.col("promo_code") == "SPRING10", 1).otherwise(0).alias("is_spring10"),
        F.concat_ws(",", "channel", "promo_code").alias("channel_promo_code_key"),
    )
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "promo_code", "starts_on", "is_spring10", "channel_promo_code_key")
)
