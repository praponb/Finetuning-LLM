from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("plan").orderBy("end_date", "id")
w3 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_5_prior"),
        F.row_number().over(w2).alias("row_seq"),
        F.last("monthly_fee", ignorenulls=True).over(w3).alias("last_known_monthly_fee"),
    )
)
