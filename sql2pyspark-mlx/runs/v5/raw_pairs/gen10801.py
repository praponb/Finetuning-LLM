from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country", "city").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("city") == "Tokyo")
    .select(
        "id",
        "supplier_name",
        "city",
        F.lead("rating").over(w_running).alias("next_rating"),
        F.lag("rating").over(w_running).alias("prev_rating"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)
