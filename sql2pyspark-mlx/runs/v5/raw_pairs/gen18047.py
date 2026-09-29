from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("is_welcome5", "id")
w2 = Window.partitionBy("promotion_promo_code").orderBy("rn", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("promotion_promo_code").orderBy("running_min_is_welcome5", "id", "promo_id")
w_hist = Window.partitionBy("promotion_promo_code")

valid = (
    df_promotions
    .filter(F.col("ends_on") >= F.to_date(F.lit("2023-08-02")))
    .withColumn("is_welcome5", F.when(F.col("promo_code") == "WELCOME5", 1).otherwise(0))
    .withColumn("is_spring10", F.when(F.col("promo_code") == "SPRING10", 1).otherwise(0))
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_window = (
    latest
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

enriched = (
    with_window
    .select(
        "id",
        "starts_on",
        "ends_on",
        "promo_id",
        "promotion_promo_code",
        F.min("is_welcome5").over(w2).alias("running_min_is_welcome5"),
        F.max("is_welcome5").over(w2).alias("running_max_is_welcome5"),
        F.sum("is_welcome5").over(w2).alias("running_sum_is_welcome5"),
    )
)

result = (
    enriched
    .filter(F.col("promo_id") < 9)
    .select(
        "id",
        "promo_id",
        "promotion_promo_code",
        "running_sum_is_welcome5",
        F.lag("running_sum_is_welcome5").over(w_grp).alias("prev_running_sum_is_welcome5"),
        F.lead("running_max_is_welcome5").over(w_grp).alias("next_running_max_is_welcome5"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.sum("running_max_is_welcome5").over(w_hist).alias("promotion_promo_code_sum_running_max_is_welcome5"),
    )
)
