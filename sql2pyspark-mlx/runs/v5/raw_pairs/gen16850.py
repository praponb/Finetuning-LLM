from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "monthly_fee",
        "customer_id",
        F.first("customer_id").over(w).alias("first_customer_id"),
    )
    .orderBy(F.col("first_customer_id").desc(), "status")
)
