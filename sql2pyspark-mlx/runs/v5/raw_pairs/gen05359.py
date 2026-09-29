from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNotNull())
    .select(
        "id",
        "plan",
        "end_date",
        F.lag("monthly_fee").over(w).alias("prev_monthly_fee"),
        F.row_number().over(w).alias("row_seq"),
    )
)
