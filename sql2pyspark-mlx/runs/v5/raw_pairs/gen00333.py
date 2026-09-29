from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .select(
        "id",
        "ends_on",
        F.count("discount_pct").over(w).alias("running_count_discount_pct"),
        F.max("discount_pct").over(w).alias("running_max_discount_pct"),
        F.min("discount_pct").over(w).alias("running_min_discount_pct"),
        F.avg("discount_pct").over(w).alias("running_avg_discount_pct"),
        F.sum("discount_pct").over(w).alias("running_discount_pct"),
    )
)
