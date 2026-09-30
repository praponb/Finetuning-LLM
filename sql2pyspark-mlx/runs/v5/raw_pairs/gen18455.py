from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("host", "service").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key")
w5 = Window.partitionBy("key", "level").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "host",
        "status_code",
        "key",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.cume_dist().over(w2).alias("status_code_cume_dist"),
        F.first("key", ignorenulls=True).over(w3).alias("first_known_key"),
        F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4).alias("status_code_share_2"),
        F.first("key", ignorenulls=True).over(w5).alias("first_known_key_2"),
    )
)
