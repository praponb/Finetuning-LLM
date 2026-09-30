from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc(), "id")

deduped = (
    df_promotions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "dedup_rank",
        F.when(F.col("promo_code").rlike("^(BUNDLE|WELCOME5)$"), "GROUP_A").otherwise("EXTENDED").alias("promo_code_zone"),
    )
    .orderBy(F.col("promo_code").desc_nulls_last(), F.col("promo_code_zone").desc())
)
