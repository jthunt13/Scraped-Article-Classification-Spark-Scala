import matplotlib.pyplot as plt
import pandas as pd

names = ["Number Of Results","Logistic Regression Train","Logistic Regression Test","Random Forest Train","Random Forest Test"]

df = pd.read_csv("./results.csv",names = names)

df.plot(x = "Number Of Results")

plt.legend()
plt.grid()
plt.savefig("../figs/modelAccuracy")
plt.show()
