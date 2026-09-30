from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(F.date_trunc("YEAR", "starts_on").alias("year_start"))
    .agg(
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
    .orderBy(F.col("year_start").desc_nulls_first())
)
