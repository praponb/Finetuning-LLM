from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "region",
        "sqft",
        "opened_date",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 3).alias("sqft_share"),
        F.lag("sqft", 1, 0).over(w2).alias("prev_sqft"),
    )
)
