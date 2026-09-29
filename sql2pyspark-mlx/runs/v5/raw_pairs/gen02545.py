from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .filter(F.month("opened_date").isin([1, 9, 12]))
    .select(
        "id",
        "city",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.sum("sqft").over(w2).alias("running_sqft"),
    )
)
