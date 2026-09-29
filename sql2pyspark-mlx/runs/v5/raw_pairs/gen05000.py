from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .filter(F.upper("email").like("%ANY%"))
    .select(
        "id",
        "level",
        "hire_date",
        "full_name",
        F.first("full_name").over(w).alias("first_full_name"),
    )
    .orderBy("level")
)
