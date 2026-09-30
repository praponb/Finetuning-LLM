from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")

with_windows = (
    df_promotions
    .select(
        "id",
        "discount_pct",
        F.ntile(10).over(w).alias("discount_pct_bucket"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    with_windows
    .groupBy("seq_no")
    .agg(
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") > 1)
    .orderBy(F.col("event_count").desc())
)
