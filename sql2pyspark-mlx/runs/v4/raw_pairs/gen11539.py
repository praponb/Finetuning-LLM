from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city")
w2 = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "region",
        "city",
        "sqft",
        "opened_date",
        F.max("sqft").over(w).alias("region_max_sqft"),
        F.lead("sqft", 1).over(w2).alias("next_sqft"),
    )
)
