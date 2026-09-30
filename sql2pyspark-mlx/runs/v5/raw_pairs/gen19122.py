from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy("end_date")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        F.rank().over(w).alias("end_date_rank"),
    )
)
