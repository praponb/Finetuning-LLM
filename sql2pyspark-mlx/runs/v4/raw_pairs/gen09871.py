from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")

cleaned = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-08-04").cast("date"))
    .select("*", F.date_add("starts_on", 1).alias("starts_on_plus_1d"))
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

cleaned_promotions = (
    ranked
    .filter(F.col("starts_on_plus_1d") > "2023-03-05")
    .select(
        "*",
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

result = (
    cleaned_promotions
    .groupBy("channel")
    .pivot("promo_code", ["SPRING10", "BUNDLE", "WELCOME5"])
    .agg(F.avg("days_since_ends_on").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "channel",
        F.col("SPRING10_amount").alias("spring10_amount"),
        F.col("SPRING10_n").alias("spring10_n"),
        F.col("BUNDLE_amount").alias("bundle_amount"),
        F.col("BUNDLE_n").alias("bundle_n"),
        F.col("WELCOME5_amount").alias("welcome5_amount"),
        F.col("WELCOME5_n").alias("welcome5_n"),
    )
)
