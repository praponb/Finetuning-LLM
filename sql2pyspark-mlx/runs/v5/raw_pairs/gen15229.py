from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.last("sqft").over(w).alias("last_sqft"),
    )
)
