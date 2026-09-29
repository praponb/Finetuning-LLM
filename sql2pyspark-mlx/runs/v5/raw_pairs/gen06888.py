from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .filter(~F.col("channel").isin(["store", "mobile", "web"]))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.count("discount_pct").over(w).alias("running_count_discount_pct"),
        F.sum("discount_pct").over(w).alias("running_sum_discount_pct"),
        F.max("discount_pct").over(w).alias("running_max_discount_pct"),
    )
)
