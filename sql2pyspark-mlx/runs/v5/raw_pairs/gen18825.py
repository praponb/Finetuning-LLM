from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.round(F.avg("sqft").over(w), 2).alias("sqft_moving_avg_4"),
    )
)
