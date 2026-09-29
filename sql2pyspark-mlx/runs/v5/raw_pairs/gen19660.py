from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .filter(F.month("ends_on").isin([1, 3, 8]))
    .select(
        "id",
        "discount_pct",
        F.max("discount_pct").over(w).alias("running_max_discount_pct"),
        F.min("discount_pct").over(w).alias("running_min_discount_pct"),
        F.avg("discount_pct").over(w).alias("running_avg_discount_pct"),
    )
)
