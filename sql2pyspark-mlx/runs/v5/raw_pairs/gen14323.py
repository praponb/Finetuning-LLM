from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w2 = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("channel").orderBy("starts_on", "id")

enriched = (
    df_promotions
    .select(
        "id",
        "starts_on",
        "discount_pct",
        F.lag("discount_pct", 2).over(w_grp).alias("prev_discount_pct"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.first("channel", ignorenulls=True).over(w2).alias("first_known_channel"),
        F.row_number().over(w_running).alias("row_seq_2"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("row_seq") >= 8, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("prev_discount_pct"), 2).alias("avg_prev_discount_pct"),
        F.count("*").alias("row_count"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.sum("discount_pct").alias("total_discount_pct"),
    )
    .orderBy(F.col("row_seq_tier").desc())
)
