from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.lag("rating").over(w).alias("prev_rating"),
        F.lead("rating").over(w).alias("next_rating"),
        F.row_number().over(w).alias("seq_no"),
        F.sum("rating").over(w2).alias("running_rating"),
    )
)
