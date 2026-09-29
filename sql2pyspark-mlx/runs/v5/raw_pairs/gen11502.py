from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & F.col("budget").isNull())
    .select("*", F.initcap(F.trim("location")).alias("location_title"))
)

result = (
    cleaned
    .select(
        "id",
        "dept_name",
        "budget",
        "location_title",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.ntile(4).over(w2).alias("budget_bucket"),
    )
)
