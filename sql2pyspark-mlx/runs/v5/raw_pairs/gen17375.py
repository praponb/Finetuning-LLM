from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "start_date",
        "end_date",
        F.first("end_date").over(w).alias("first_end_date"),
    )
    .orderBy("first_end_date")
)
