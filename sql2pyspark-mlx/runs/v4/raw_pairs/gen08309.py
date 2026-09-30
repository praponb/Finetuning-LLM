from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("opened_date").between("2025-07-02", "2025-07-10"))
    .select("id", "opened_date", F.row_number().over(w).alias("opened_date_row"))
)
