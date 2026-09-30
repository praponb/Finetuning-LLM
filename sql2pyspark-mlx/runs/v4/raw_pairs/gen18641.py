from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(100, 190))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
)
