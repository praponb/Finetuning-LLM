from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "TH", "JP"]))
    .select(
        "id",
        "country",
        "rating",
        "supplier_name",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
)
