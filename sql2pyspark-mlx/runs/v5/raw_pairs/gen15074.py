from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["EXPIRED", "CANCELLED"]))
    .select(
        "id",
        "status",
        "end_date",
        "monthly_fee",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
    )
)
