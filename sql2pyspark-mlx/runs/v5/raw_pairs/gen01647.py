from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("end_date", "id")
w2 = Window.partitionBy("status", "customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_subscriptions
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.first("monthly_fee", ignorenulls=True).over(w2).alias("first_known_monthly_fee"),
    )
)

result = enriched.select("id", "customer_id", "plan")
