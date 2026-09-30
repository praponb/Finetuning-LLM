from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "JP"]))
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.first("country").over(w).alias("first_country"),
    )
)
