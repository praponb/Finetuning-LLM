from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_int").desc(), "id")
w2 = Window.partitionBy("channel")

filtered = (
    df_promotions
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-08-10"))) & F.col("channel").isNotNull() & (F.col("discount_pct") <= 0.22))
    .withColumn("channel_prefix", F.substring("channel", 1, 2))
    .withColumn("discount_pct_int", F.col("discount_pct").cast("int"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("OTHER")))
)

result = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
    )
)
