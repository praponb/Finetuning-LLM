from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        F.lag("rating").over(w_running).alias("prev_rating"),
        F.lead("rating").over(w_running).alias("next_rating"),
        F.row_number().over(w_running).alias("event_seq"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
