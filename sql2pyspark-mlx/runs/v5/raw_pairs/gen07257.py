from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("end_date", "id")
w2 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("monthly_fee").desc())
w_recent = Window.orderBy(F.col("start_date").desc(), "id")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.lead("monthly_fee", 1).over(w).alias("next_monthly_fee"),
        F.last("monthly_fee", ignorenulls=True).over(w2).alias("last_known_monthly_fee"),
        F.dense_rank().over(w3).alias("monthly_fee_dense_rank"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)
