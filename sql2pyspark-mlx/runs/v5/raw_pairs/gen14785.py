from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")
w2 = Window.partitionBy("city", "region").orderBy("opened_date")

result = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "region",
        "sqft",
        "city",
        "opened_date",
        F.row_number().over(w).alias("rn"),
        F.rank().over(w2).alias("rnk"),
    )
)
