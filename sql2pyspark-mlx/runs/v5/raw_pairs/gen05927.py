from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_suppliers
    .select(
        "*",
        F.lag("rating").over(w_ordered).alias("prev_rating"),
        F.lead("rating").over(w_ordered).alias("next_rating"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.first("rating", ignorenulls=True).over(w2).alias("first_known_rating"),
    )
)
