from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy("start_date", "id")
w2 = Window.partitionBy("customer_id", "plan")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.lag("monthly_fee").over(w_grp).alias("prev_monthly_fee"),
        F.lead("monthly_fee").over(w_grp).alias("next_monthly_fee"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.sum("monthly_fee").over(w2).alias("customer_id_sum_monthly_fee"),
    )
)
