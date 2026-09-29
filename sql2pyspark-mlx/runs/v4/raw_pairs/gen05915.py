from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date")

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        "city",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "region", "opened_date", "city")
)
