from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("city", "region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "region",
        "sqft",
        "city",
        "opened_date",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 3).alias("sqft_share"),
        F.lead("sqft", 1).over(w2).alias("next_sqft"),
    )
)
