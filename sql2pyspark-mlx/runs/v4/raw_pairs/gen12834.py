from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location")

metrics = (
    df_departments
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("budget_bucket", F.ntile(4).over(w_recent))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
)

result = (
    metrics
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "row_seq",
        "budget_bucket",
        "budget_share",
        F.when(F.col("location").rlike("^(Singapore|New York)$"), "EXTENDED").when(F.col("location").rlike("^(Bangkok|Chiang Mai)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("location_zone"),
    )
)
