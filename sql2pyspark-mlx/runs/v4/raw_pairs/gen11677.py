from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .select(
        "id",
        "level",
        "hire_date",
        "full_name",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "level", "hire_date", "full_name")
    .orderBy("full_name")
)
