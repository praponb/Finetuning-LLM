from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "promo_code")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id")

scored = (
    df_promotions
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 2).over(w3))
)

result = (
    scored
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "cumulative_discount_pct",
        "discount_pct_share",
        "prev_discount_pct",
        F.when(F.col("promo_code").rlike("^(SPRING10|BUNDLE)$"), "EXTENDED").otherwise("GROUP_A").alias("promo_code_zone"),
    )
)
