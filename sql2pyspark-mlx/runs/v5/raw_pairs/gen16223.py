from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc(), "id")

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_upper", F.upper("location"))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("OTHER")))
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
