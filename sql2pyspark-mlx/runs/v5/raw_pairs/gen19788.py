from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("end_date").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "status",
        "end_date",
        "start_date",
        F.ntile(4).over(w).alias("end_date_ntile"),
        F.last("end_date").over(w2).alias("last_end_date"),
    )
)
