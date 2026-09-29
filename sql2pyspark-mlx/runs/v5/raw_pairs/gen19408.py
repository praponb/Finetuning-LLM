from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_suppliers
    .filter(F.length("supplier_name") >= 7)
    .select(
        "id",
        "country",
        "rating",
        F.sum("rating").over(w).alias("country_sum_rating"),
    )
    .orderBy(F.col("rating").desc())
)
