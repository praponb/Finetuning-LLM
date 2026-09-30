from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("channel").orderBy("discount_pct_int", "id")
w2 = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc(), "id")
w4 = Window.orderBy(F.col("starts_on").desc(), "id")
w5 = Window.orderBy("row_seq", "id")
w6 = Window.partitionBy("promo_code")
w7 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct_share").desc_nulls_last())

valid = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2023-10-03").cast("date"))
    .withColumn("channel_upper", F.upper("channel"))
    .withColumn("discount_pct_int", F.col("discount_pct").cast("int"))
)

scored = (
    valid
    .withColumn("next_discount_pct", F.lead("discount_pct").over(w_hist))
    .withColumn("prev_discount_pct", F.lag("discount_pct").over(w_hist))
    .withColumn("prev_discount_pct_int", F.lag("discount_pct_int").over(w_hist))
    .withColumn("row_seq", F.row_number().over(w_hist))
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w2))
)

enriched = (
    scored
    .select(
        "id",
        "channel",
        "promo_code",
        "prev_discount_pct",
        "prev_discount_pct_int",
        "row_seq",
        "cumulative_discount_pct",
        F.ntile(4).over(w3).alias("discount_pct_bucket"),
        F.row_number().over(w4).alias("row_seq_2"),
        F.ntile(4).over(w5).alias("row_seq_bucket"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w6)), 4).alias("discount_pct_share"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("cumulative_discount_pct") >= 2, "PREMIUM").otherwise("STANDARD").alias("cumulative_discount_pct_tier"),
    )
    .agg(
        F.avg(F.when(F.col("channel") == "partner", F.col("row_seq_bucket"))).alias("partner_avg_row_seq_bucket"),
        F.max("row_seq_2").alias("max_row_seq_2"),
        F.round(F.avg("discount_pct_bucket"), 2).alias("avg_discount_pct_bucket"),
    )
    .orderBy("max_row_seq_2")
)
