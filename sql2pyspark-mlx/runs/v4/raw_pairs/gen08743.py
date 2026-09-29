from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "monthly_fee",
        "customer_id",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "status", "monthly_fee", "customer_id")
    .orderBy("status")
)
