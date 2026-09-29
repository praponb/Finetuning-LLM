from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .pivot("level", ["L1", "L5", "L3"])
    .agg(F.count("salary"))
    .orderBy("is_remote")
)
