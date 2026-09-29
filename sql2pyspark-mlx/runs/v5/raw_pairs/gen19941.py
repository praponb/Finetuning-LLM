from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "status",
        F.last("status").over(w).alias("last_status"),
    )
    .orderBy("id")
)
