#%%

matrix = [
    "qwertyuiop",
    "asdfghjkl;",
    "zxcvbnm,./"
]

pairs = []

for a in range(len(matrix)) :
    for i in range(len(matrix[a])):
        if i + 1 < len(matrix[a]):
            pairs.append((matrix[a][i], matrix[a][i + 1]))

        if a + 1 < len(matrix):
            pairs.append((matrix[a][i], matrix[a + 1][i]))

        if a + 1 < len(matrix) and i + 1 < len(matrix[a]):
            pairs.append((matrix[a][i], matrix[a + 1][i + 1]))

        if a + 1 < len(matrix) and i - 1 >= 0:
            pairs.append((matrix[a][i], matrix[a + 1][i - 1]))

pairs
#%%

import networkx as nx
import itertools as it

G = nx.Graph()
G.add_nodes_from(it.chain.from_iterable(matrix))
G.add_edges_from(pairs)

#%%

import pandas as pd
import json

df = pd.read_csv("./keystats/bigrams_cleaned_count.csv")
df = df.query("type == 'LETTER' and following_type == 'LETTER'")
df["edges"] = df.bigram.apply(lambda b: tuple(e.lower() for e in json.loads(b.replace("'", '"'))))

reverseddf = df.assign(edges=df["edges"].apply(lambda e: tuple(reversed(e))))

dupdf = pd.concat([df, reverseddf])
dupdf = dupdf.groupby("edges").sum()

# %%

def add_edge_count(row):
    global G
    try:
        G.edges[row["edges"]]["weight"] = row["count"]
    except KeyError as e: pass

dupdf.apply(add_edge_count, axis=1)

#%%

import matplotlib.pyplot as plt
import matplotlib.colors as colors

cdict = {'red':  ((0.0, 0.0, 0.0),   # no red at 0
                  (0.5, 1.0, 1.0),   # all channels set to 1.0 at 0.5 to create white
                  (1.0, 0.8, 0.8)),  # set to 0.8 so its not too bright at 1

        'green': ((0.0, 0.8, 0.8),   # set to 0.8 so its not too bright at 0
                  (0.5, 1.0, 1.0),   # all channels set to 1.0 at 0.5 to create white
                  (1.0, 0.0, 0.0)),  # no green at 1

        'blue':  ((0.0, 0.0, 0.0),   # no blue at 0
                  (0.5, 1.0, 1.0),   # all channels set to 1.0 at 0.5 to create white
                  (1.0, 0.0, 0.0))   # no blue at 1
       }

# Create the colormap using the dictionary
GnRd = colors.LinearSegmentedColormap('GnRd', cdict)
GnRd = plt.get_cmap("bwr")

plt.figure(figsize=(10,10))

coords = {}
for a in range(len(matrix)) :
    for i in range(len(matrix[a])):
        coords[matrix[a][i]] = (a, i)

pos = {n: (coords[n][1], -coords[n][0]) for n in G.nodes()}

labels = nx.get_edge_attributes(G,'weight')

M, m = max(labels.values()), min(labels.values())
# M, m = max(list(sorted(labels.values())[:-10])), min(labels.values())
colors = [GnRd((v - m) / (M - m)) for v in labels.values()]

# pos = nx.spring_layout(G, iterations=100, seed=39775)
nx.draw(G, pos, edge_color=colors, with_labels=True, font_weight='bold', node_size=600)

nx.draw_networkx_edge_labels(G,pos,edge_labels=labels, label_pos=0.4)

#%%