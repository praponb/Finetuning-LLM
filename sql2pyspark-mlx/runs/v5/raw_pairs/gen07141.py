from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc(), "id")
w2 = Window.orderBy("opened_date", "id")
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_stores
    .filter(F.col("opened_date").between("2025-08-01", "2025-10-01"))
    .withColumn("sqft_bucket", F.ntile(3).over(w))
    .withColumn("next_sqft", F.lead("sqft", 1).over(w2))
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w3))
)

result = (
    scored
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft_bucket",
        "next_sqft",
        "first_known_region",
        F.when(F.col("store_name").rlike("^(Mall Central|Downtown Express)$"), "GROUP_B").otherwise("GROUP_A").alias("store_name_zone"),
    )
    .orderBy(F.col("store_name_zone").desc())
)
