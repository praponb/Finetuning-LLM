from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")
w2 = Window.partitionBy("year_start").orderBy(F.col("row_count").desc())

latest = (
    df_promotions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("YEAR", "starts_on").alias("year_start"),
        F.date_trunc("QUARTER", "starts_on").alias("quarter_start"),
    )
    .agg(
        F.count(F.when(F.col("promo_code") == "BUNDLE", 1)).alias("bundle_count"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sort_array(F.collect_set("channel")).alias("channel_list_2"),
    )
    .withColumn("row_count_dense_rank", F.dense_rank().over(w2))
    .orderBy(F.col("bundle_count").desc_nulls_first())
)
