from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")

deduped = (
    df_promotions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy("promo_code")
    .agg(
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("discount_pct_per_store"),
    )
)
