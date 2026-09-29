from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_month").orderBy(F.col("store_count").desc())

kept_keys = (
    df_promotions
    .select("id")
    .intersect(
        df_promotions
        .filter(F.col("promo_code") == "WELCOME5")
        .select("id")
    )
)

kept_rows = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("month", "ends_on").alias("activity_month"), "promo_code")
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_count"),
    )
    .withColumn("store_count_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("store_count").desc())
)
