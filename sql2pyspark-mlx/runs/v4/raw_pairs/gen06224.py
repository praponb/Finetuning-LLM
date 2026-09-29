from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.length("supplier_name") > 13)
    .select(
        "id",
        "rating",
        "country",
        F.lead("rating", 1).over(w).alias("next_rating"),
        F.row_number().over(w2).alias("rating_row"),
    )
)
