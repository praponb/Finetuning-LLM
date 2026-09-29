from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "region",
        F.first("region").over(w).alias("first_region"),
    )
)
