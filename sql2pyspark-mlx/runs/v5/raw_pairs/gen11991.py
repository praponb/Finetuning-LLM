from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "end_date",
        F.percent_rank().over(w).alias("end_date_pct_rank"),
    )
)
