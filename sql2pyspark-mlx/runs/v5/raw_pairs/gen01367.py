from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id")
w2 = Window.orderBy(F.col("delivered_date").desc(), "id")

valid = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-03-18"))) & F.col("carrier").isNotNull())
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("cost") >= 83, "High").when(F.col("cost") >= 22, "Medium").otherwise("Low").alias("cost_level"),
    )
)

result = (
    valid
    .select(
        "id",
        "shipped_date",
        "cost",
        "status",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
