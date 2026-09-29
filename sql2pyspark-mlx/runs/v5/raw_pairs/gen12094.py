from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("discount_pct")
w_running = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("ends_on", "id")
w4 = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")

result = (
    df_promotions
    .select(
        "*",
        F.percent_rank().over(w_grp).alias("discount_pct_percent_rank"),
        F.sum("discount_pct").over(w_running).alias("running_discount_pct"),
        F.lead("discount_pct", 1).over(w3).alias("next_discount_pct"),
        F.row_number().over(w4).alias("event_seq"),
    )
)
