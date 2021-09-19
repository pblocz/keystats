
#%%

#**
#** Define matrices
#**

import itertools as it
import math

from numpy import kaiser
#%%

matrix = [
    "1qwertyuiop4",
    "2asdfghjkl;5",
    "3zxcvbnm,./6"
]

home_keys = "asdfjkl;"

# %%

#**
#** Define aux functions and list
#**

list_positions = range(len(list(it.chain.from_iterable(matrix))))
list_matrix = list(it.chain.from_iterable(matrix))
pos_combination = list(it.combinations(list_positions, 2))


def p2pm(p: int, rows=len(matrix), cols=len(matrix[0])) -> int:
    return (math.floor(p / cols), p % cols)

def p2m(p: int, rows=len(matrix), cols=len(matrix[0])) -> int:
    x, y = p2pm(p, rows, cols)
    return matrix[x][y]

def m2p(p: str, rows=len(matrix), cols=len(matrix[0])) -> tuple:
    return list_matrix.index(p)

def m2pm(p: str, rows=len(matrix), cols=len(matrix[0])) -> tuple:
    return p2pm(m2p(p, rows, cols), rows, cols)

def pmdist(a: tuple, b: tuple) -> int:
    return abs(a[0] - b[0]) + abs(a[1] - b[1])

def hrdist(a: tuple):
    return min((pmdist(a, m2pm(k)), k) for k in home_keys)


#%%

#**
#** Define combos
#**

from dataclasses import dataclass, field
move_effort_home = [6,4,2,1,1,2,4,6,]
move_effort_dict = dict(zip(home_keys, move_effort_home))

press_effort_home = [5,3,0,0,0,0,3,5,]
press_effort_dict = dict(zip(home_keys, press_effort_home))

hand = {
    "a": 0,
    "s": 0,
    "d": 0,
    "f": 0,
    "j": 1,
    "k": 1,
    "l": 1,
    ";": 1,
}

@dataclass
class combo:
    kp1: str
    kp2: str

    effort1: int
    effort2: int

    home_finger1: str
    home_finger2: str

    effort_score: int = field(init=False)

    def __post_init__(self):
        self.effort_score = (
            move_effort_dict[self.home_finger1] * self.effort1 +
            move_effort_dict[self.home_finger2] * self.effort2 +

            press_effort_dict[self.home_finger1] +
            press_effort_dict[self.home_finger2] +

            # Different hand effort penalty
            (0 if hand[self.home_finger1] == hand[self.home_finger2] else 1)
        )

combo_dist = [( (p2m(a), p2m(b)), (hrdist(p2pm(a)), hrdist(p2pm(b))) ) for a, b in pos_combination]
combos = [combo(*kps, ds[0][0], ds[1][0], ds[0][1], ds[1][1] )for kps, ds in combo_dist]

#%%

#**
#** Create spark context
#**

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

import pandas as pd

sc = SparkContext('local', 'test')
spark = SparkSession(sc)

#%%

efdf = (
    spark.createDataFrame(pd.DataFrame(combos))
    .filter(F.col("home_finger1") != F.col("home_finger2"))
    .sort("effort_score")
)

efdf.toPandas()
# %%

efdf.toPandas().to_csv("./possible-combos.csv", index=False)

#%%