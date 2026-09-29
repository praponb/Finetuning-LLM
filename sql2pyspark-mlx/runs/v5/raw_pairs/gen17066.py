from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id").rowsBetween(-6, Window.currentRow)

scored = (
    df_promotions
    .filter(F.col("ends_on") < "2025-06-01")
    .select(
        "id",
        "starts_on",
        "discount_pct",
        F.count("discount_pct").over(w_grp).alias("last7_count_discount_pct"),
        F.max("discount_pct").over(w_grp).alias("last7_max_discount_pct"),
        F.min("discount_pct").over(w_grp).alias("last7_min_discount_pct"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("last7_count_discount_pct") >= 14, "HIGH").otherwise("LOW").alias("last7_count_discount_pct_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.percentile_approx("last7_min_discount_pct", 0.95).alias("last7_min_discount_pct_p95"),
    )
    .filter(F.col("row_count") >= 1)
    .orderBy("row_count")
)
