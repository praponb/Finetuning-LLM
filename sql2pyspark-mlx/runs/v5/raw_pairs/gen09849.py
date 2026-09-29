from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_sessions
    .select(
        "id",
        "country",
        "started_at",
        "browser",
        F.last("browser").over(w).alias("last_browser"),
    )
)
