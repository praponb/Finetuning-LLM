from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "monthly_fee",
        "end_date",
        F.last("end_date").over(w).alias("last_end_date"),
    )
)
