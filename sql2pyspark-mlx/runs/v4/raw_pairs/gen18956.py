from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("promo_code")
w3 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter(F.col("ends_on") > F.lit("2024-03-28").cast("date"))
    .withColumn("clean_channel", F.trim("channel"))
)

result = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.avg("discount_pct").over(w2).alias("promo_code_avg_discount_pct"),
        F.sum("discount_pct").over(w3).alias("cumulative_discount_pct"),
    )
)
