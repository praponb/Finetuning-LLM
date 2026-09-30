from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id")
w2 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("city", "region")

result = (
    df_stores
    .select(
        "id",
        "city",
        "sqft",
        F.lead("sqft", 1).over(w).alias("next_sqft"),
        F.avg("sqft").over(w2).alias("sqft_avg_2"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
    )
)
