from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc(), "id")

cleaned = (
    df_promotions
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-05-13"))) & F.col("channel").isNotNull() & (F.col("channel") == "store"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.initcap(F.trim("promo_code")).alias("promo_code_title"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)
