from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "started_at",
        "browser",
        F.last("browser").over(w).alias("last_browser"),
    )
    .orderBy(F.col("id").desc(), "browser")
)
