from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "plan",
        F.last("plan").over(w).alias("last_plan"),
    )
)
