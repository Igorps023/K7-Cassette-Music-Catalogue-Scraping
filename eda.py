# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme()


# %%
def Generate_Metadata(dataframe):
    """
    Gera um dataframe contendo metadados das colunas do dataframe fornecido.

    :param dataframe: DataFrame para o qual os metadados serão gerados.
    :return: DataFrame contendo metadados.
    """

    # Coleta de metadados básicos
    metadata = pd.DataFrame(
        {
            "nome_variavel": dataframe.columns,
            "tipo": dataframe.dtypes.astype(
                str
            ),  # Convert to string #dataframe.dtypes,
            "qt_nulos": dataframe.isnull().sum(),
            "percent_nulos": round(
                (dataframe.isnull().sum() / len(dataframe)) * 100, 2
            ),
            "cardinalidade": dataframe.nunique(),
        }
    )
    metadata = metadata.sort_values(by="tipo")
    metadata = metadata.reset_index(drop=True)

    return metadata


# %%
# Reading data
music_catalogue = "/home/igor/Desktop/K7-Cassette-Music-Catalogue-Scraping/data_selenium/20240704_093039_187"
df = pd.read_parquet(music_catalogue)

# df.shape # Rows and Columns
# df.dtypes # Types such as float, object
# %%
# Adding column "currency" on index 3
df.insert(3, "currency", "R$")

# Treating and converting datatype
df["price"] = (
    df["price"]
    .str.replace("R$", "", regex=False)
    .str.replace(",", ".", regex=False)
    .str.strip()
)
df["price"] = pd.to_numeric(df["price"])
# %%
# %%
df["price"].describe()  # Returns a dataframe with statistics about the dataframe
# %%
# Interpretação e análise da variável preço
# Boxplot
plt.figure(figsize=(8, 6))
df.boxplot(column="price")
plt.title("Boxplot da Variável - Price")
plt.ylabel("Valores")
plt.show()
# %%
df.info()  # Checks if any of the columns contains null values and dtype
# %%
Generate_Metadata(df)  # Function that generates metadata
# %%
# Creating the histogram
plt.figure(figsize=(10, 6))
plt.hist(df["price"], bins=70, edgecolor="grey", alpha=0.7, color="lightblue")
plt.title("Distribuição de preços")
plt.xlabel("Preço (R$)")
plt.ylabel("Frequência")

# Ticks interval on x axis
max_price = df["price"].max()
plt.xticks(np.arange(0, max_price + 1, step=40), rotation=0)

plt.grid(axis="y")
plt.tight_layout()
plt.show()
# %%
# V2 Histogram with KDE included
plt.figure(figsize=(10, 6))
sns.histplot(
    df["price"],
    bins=70,
    edgecolor="grey",
    alpha=0.7,
    color="skyblue",
    kde=True,
    line_kws={"color": "green", "linestyle": "solid"},
)
plt.title("Distribuição de preços")
plt.xlabel("Preço (R$)")
plt.ylabel("Frequência")

# Definindo os intervalos dos ticks no eixo x
max_price = df["price"].max()
plt.xticks(np.arange(0, max_price + 1, step=40), rotation=0)

plt.grid(axis="y")
plt.tight_layout()
plt.show()
# %%
# Top 20 Expensiviest K7s listed
df[["artist", "title", "currency", "price"]].sort_values(
    by=["price"],
    ascending=False,
).head(20)

# %%
# Top 10 Cheapest K7s listed
df[["artist", "title", "currency", "price"]].sort_values(
    by=["price"],
    ascending=True,
).head(10)
