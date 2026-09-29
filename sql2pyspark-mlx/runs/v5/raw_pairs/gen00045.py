from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc())
w2 = Window.partitionBy("city").orderBy("opened_date")

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        "city",
        F.rank().over(w).alias("rnk"),
        F.rank().over(w2).alias("rnk_2"),
    )
)
