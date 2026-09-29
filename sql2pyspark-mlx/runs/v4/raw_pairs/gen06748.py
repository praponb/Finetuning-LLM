from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date")
w2 = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        "city",
        "sqft",
        F.rank().over(w).alias("opened_date_rank"),
        F.lead("sqft", 1).over(w2).alias("next_sqft"),
    )
    .orderBy("next_sqft", "opened_date")
)
