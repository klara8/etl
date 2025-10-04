import matplotlib as mlib
import seaborn as sb
import pandas as pd
from matplotlib import pyplot as plt

df = pd.read_csv("/Users/klarakopecka/ETL_project/etl/dataL/dataL.csv")

#graph of discount labels
count_great = df[df["disc_label"] == "great discount"].shape[0]
count_good = df[df["disc_label"] == "good discount"].shape[0]
count_ok = df[df["disc_label"] == "ok discount"].shape[0]
count_notgreat = df[df["disc_label"] == "not that great discount"].shape[0]
count_nan = df[df["disc_label"].isna()].shape[0]

#print(count_great+count_good+count_ok+count_notgreat)
#print(len(df))
labels = ["great","good","ok","not that great","missing"]
values = [count_great,count_good,count_ok,count_notgreat,count_nan]

plt.pie(
    values,
    labels=labels

)
plt.title("discounts")
plt.show()
