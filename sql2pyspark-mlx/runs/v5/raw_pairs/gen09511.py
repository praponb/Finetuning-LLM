from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")

cleaned = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2023-06-13").cast("date"))
    .select(
        "id",
        "promo_code",
        "starts_on",
        F.date_format("starts_on", "yyyy-MM").alias("starts_on_label"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "promo_code", "starts_on_label")
)

kept_keys = (
    latest
    .select("promo_code", "id")
    .subtract(
        df_promotions
        .filter(F.col("ends_on") >= "2025-10-23")
        .select("promo_code", "id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned_promotions = (
    reconciled
    .filter(F.col("promo_code") == "SPRING10")
    .withColumn("clean_promo_code", F.trim("promo_code"))
    .withColumn("promo_code_group", F.when(F.col("promo_code").isin(["VIP", "SPRING10"]), "Core").otherwise("Other"))
)

result = cleaned_promotions.select("clean_promo_code")
