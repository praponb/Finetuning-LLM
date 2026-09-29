from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("starts_on").desc(), "id")

ranked = (
    df_promotions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

ranked_promotions = (
    ranked
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

matched_keys = (
    ranked_promotions
    .select("promo_code", "channel")
    .subtract(
        df_promotions
        .filter(F.col("starts_on") > "2023-03-01")
        .select("promo_code", "channel")
    )
)

kept_rows = (
    ranked_promotions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("ends_on") >= F.lit("2023-09-21").cast("date"))
    .select("*", F.add_months("ends_on", 12).alias("ends_on_plus_months"))
)

remaining_keys = (
    valid
    .select("id", "promo_code")
    .subtract(
        df_promotions
        .filter(F.col("promo_code").isin(["WELCOME5", "SPRING10"]))
        .select("id", "promo_code")
    )
)

kept_rows_promotions = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    kept_rows_promotions
    .select("ends_on", "discount_pct", "ends_on_plus_months")
    .orderBy(F.col("discount_pct").desc())
)
