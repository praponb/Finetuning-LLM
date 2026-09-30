from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on")
w2 = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")
w3 = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc())

ranked = (
    df_promotions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "promo_code", "ends_on")
)

latest = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

filtered = (
    latest
    .filter(F.col("ends_on") <= "2023-05-23")
    .select("*", F.date_sub("ends_on", 39).alias("ends_on_minus_39d"))
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "promo_code")
)

result = (
    deduped
    .select(
        "id",
        "promo_code",
        F.when(F.col("promo_code").rlike("^(VIP|BUNDLE)$"), "GROUP_B").when(F.col("promo_code").rlike("^(SPRING10|WELCOME5)$"), "CORE").otherwise("PRIORITY").alias("promo_code_zone"),
    )
    .orderBy(F.col("promo_code_zone").asc_nulls_first())
)
