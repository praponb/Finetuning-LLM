from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .pivot("level", ["L2", "L3"])
    .agg(F.max("salary"))
    .orderBy("is_remote")
)
