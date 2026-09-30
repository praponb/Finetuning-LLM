from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w2 = Window.partitionBy("region").orderBy(F.col("opened_date").desc())

result = (
    df_stores
    .filter(F.col("store_name").like("%Downto%"))
    .select(
        "id",
        "opened_date",
        "sqft",
        "region",
        F.first("sqft").over(w).alias("first_sqft"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
