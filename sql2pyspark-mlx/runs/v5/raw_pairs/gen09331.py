from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.count("*").over(w).alias("country_count"),
        F.row_number().over(w2).alias("rn"),
    )
)
