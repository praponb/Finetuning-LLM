from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("starts_on").desc(), "id")
w_grp = Window.orderBy("ends_on", "id").rowsBetween(-6, -1)

scored = (
    df_promotions
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("discount_pct_avg_6_prior", F.avg("discount_pct").over(w_grp))
)

result = (
    scored
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "event_seq",
        "discount_pct_avg_6_prior",
    )
)
