from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "region",
        "opened_date",
        "store_name",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "region", "opened_date", "store_name")
    .orderBy("region")
)
