from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city")

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.lead("rating").over(w).alias("next_rating"),
        F.row_number().over(w).alias("row_seq"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
    )
)
