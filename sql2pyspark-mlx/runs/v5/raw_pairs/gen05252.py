from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .filter(F.col("city").isin(["Singapore", "Berlin", "Bangkok"]))
    .select(
        "id",
        "city",
        "opened_date",
        "store_name",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 2)
    .select("id", "city", "opened_date", "store_name")
)
