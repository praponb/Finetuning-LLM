from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.lag("sqft", 1).over(w).alias("prev_sqft"),
    )
    .orderBy("opened_date")
)
