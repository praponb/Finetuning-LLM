from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w3 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .filter(F.col("ends_on").between("2024-02-01", "2024-04-26"))
    .select(
        "id",
        "starts_on",
        "ends_on",
        F.max("discount_pct").over(w).alias("channel_max_discount_pct"),
        F.lead("discount_pct", 1).over(w2).alias("next_discount_pct"),
        F.sum("discount_pct").over(w3).alias("cumulative_discount_pct"),
    )
)
