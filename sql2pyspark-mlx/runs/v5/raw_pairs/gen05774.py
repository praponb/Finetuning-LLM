from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        F.lead("monthly_fee").over(w_running).alias("next_monthly_fee"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)
