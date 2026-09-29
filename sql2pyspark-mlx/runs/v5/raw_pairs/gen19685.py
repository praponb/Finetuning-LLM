from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w2 = Window.partitionBy("region").orderBy("sqft", "id")

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "sqft",
        "region",
        F.lead("sqft", 1).over(w).alias("next_sqft"),
        F.first("opened_date").over(w2).alias("first_opened_date"),
    )
    .orderBy(F.col("id").desc())
)
