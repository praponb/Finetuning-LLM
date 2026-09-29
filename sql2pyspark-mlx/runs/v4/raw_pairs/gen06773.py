from pyspark.sql import functions as F

result = (
    df_departments
    .select("location", F.initcap(F.trim("location")).alias("location_title"))
)
