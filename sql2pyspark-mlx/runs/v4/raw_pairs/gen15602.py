from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "status",
        "discount",
        "channel",
        F.last("channel").over(w).alias("last_channel"),
    )
)
