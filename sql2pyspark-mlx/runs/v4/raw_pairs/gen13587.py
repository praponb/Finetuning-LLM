from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_stores
    .filter(F.col("opened_date") >= "2023-11-01")
    .select(
        "id",
        "region",
        "opened_date",
        "city",
        F.last("city").over(w).alias("last_city"),
    )
)
