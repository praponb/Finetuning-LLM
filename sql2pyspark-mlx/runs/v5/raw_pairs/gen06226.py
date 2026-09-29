from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.upper(F.trim("supplier_name")) == "GLOBAL SUPPLY")
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.lead("rating", 1).over(w).alias("next_rating"),
    )
)
