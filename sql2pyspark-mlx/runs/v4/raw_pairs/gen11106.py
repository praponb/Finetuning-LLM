from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "brand",
        "launch_date",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_7"),
    )
    .orderBy(F.col("cost_moving_avg_7").desc())
)
