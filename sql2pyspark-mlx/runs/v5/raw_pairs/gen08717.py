from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy(F.col("last4_count_discount_pct").desc(), "id")
w_ordered = Window.partitionBy("promo_code").orderBy("last4_sum_discount_pct", "id")
w4 = Window.partitionBy("promo_code")
w5 = Window.orderBy("last4_count_discount_pct", "id")

valid = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-08-08").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.concat("promo_code", F.lit("/"), "channel").alias("promo_code_channel"),
    )
)

enriched = (
    valid
    .filter(F.col("promo_code").isin(["SPRING10", "WELCOME5"]))
    .select(
        "id",
        "promo_code",
        "promo_code_channel",
        F.sum("discount_pct").over(w_grp).alias("last4_sum_discount_pct"),
        F.max("discount_pct").over(w_grp).alias("last4_max_discount_pct"),
        F.count("discount_pct").over(w_grp).alias("last4_count_discount_pct"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "promo_code",
        "last4_sum_discount_pct",
        "last4_max_discount_pct",
        "last4_count_discount_pct",
    )
)

enriched_promotions = (
    deduped
    .select(
        "id",
        "promo_code",
        "last4_sum_discount_pct",
        F.ntile(4).over(w_ordered).alias("last4_sum_discount_pct_bucket"),
        F.round((F.col("last4_sum_discount_pct") / F.sum("last4_sum_discount_pct").over(w4)), 4).alias("last4_sum_discount_pct_share"),
        F.lead("last4_max_discount_pct", 2).over(w5).alias("next_last4_max_discount_pct"),
    )
)

result = (
    enriched_promotions
    .groupBy("promo_code")
    .agg(
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_count"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.max("last4_sum_discount_pct").alias("max_last4_sum_discount_pct"),
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_rows"),
        F.round(F.avg("last4_sum_discount_pct_bucket"), 2).alias("avg_last4_sum_discount_pct_bucket"),
    )
    .orderBy(F.col("avg_last4_sum_discount_pct_bucket").desc())
)
