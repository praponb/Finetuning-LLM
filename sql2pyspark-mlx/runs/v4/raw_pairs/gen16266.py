from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("opened_date") >= "2024-03-01")
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.lag("sqft", 2).over(w).alias("prev_sqft"),
    )
    .orderBy("sqft")
)
