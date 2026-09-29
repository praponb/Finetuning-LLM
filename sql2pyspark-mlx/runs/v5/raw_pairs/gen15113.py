from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.round(F.avg("sqft").over(w2), 2).alias("sqft_moving_avg_3"),
    )
    .orderBy(F.col("id").desc())
)
