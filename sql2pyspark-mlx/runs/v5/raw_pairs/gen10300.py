from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("carrier", "status").orderBy("status_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "*",
        F.length("status").alias("status_length"),
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
    )
)

result = (
    cleaned
    .withColumn("cost_bucket", F.ntile(10).over(w))
    .withColumn("first_known_carrier", F.first("carrier", ignorenulls=True).over(w2))
)
