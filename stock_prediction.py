# Instalação do Ambiente PySpark e demais dependências

# instalar as dependências
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop2.tgz 
!tar xf spark-3.3.0-bin-hadoop2.tgz

# configurar as variáveis de ambiente e o Spark
import os
import sys
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.3.0-bin-hadoop2.tgz"

!pip install -q findspark

# tornar o pyspark "importável"
import findspark
findspark.init('spark-3.3.0-bin-hadoop2')

# iniciar uma sessão local
from pyspark.sql import SparkSession,SQLContext

spark = SparkSession.builder.master("local[*]").appName("DadosBrazilianStockMarket").getOrCreate()

# Instalando o yahoo finance
!pip install yfinance --upgrade --no-cache-dir

import matplotlib.pyplot as plt

import pyspark
from pyspark.sql import SparkSession

import pyspark.pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf 
import requests

import numpy as np

from pyspark.sql.types import StructType,StructField, StringType

from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression

import plotly.offline as py
import plotly.graph_objs as go

from google.colab import files

# Aquisição de Dados

url = "https://investnews.com.br/financas/veja-a-lista-completa-dos-bdrs-disponiveis-para-pessoas-fisicas-na-b3/"
r = requests.get(url)
html = r.text
df_nomes_tickers = pd.read_html(html, header=0)[0]
df_nomes_tickers.rename(columns={'EMPRESA': 'Empresa', 'CÓDIGO': 'Codigo','SETOR': 'Setor','PAÍS DE ORIGEM': 'Pais_de_origem'}, inplace=True)
df_nomes_tickers.head(10)

df_nomes_tickers['Codigo']=df_nomes_tickers['Codigo']+'.SA'
xd=df_nomes_tickers.to_spark()
xd.show()

xd.createOrReplaceTempView("xd")

analise_total_query=spark.sql("select count(distinct Empresa) as Total_de_Empresas, count(distinct Setor) as Setores,count(distinct Pais_de_origem) as Paises  from xd")

analise_total_query.show(10,False)

ticket=xd.select('Codigo').toPandas()['Codigo']
ticket_list=list(ticket)
data=yf.Tickers(ticket_list).history(period="2y")

new_df=data.stack(1).reset_index().rename(columns={'level_1':'Ticker'})
new_dfx = pd.DataFrame(new_df)
dfx=new_dfx.to_spark()
dfx.show()

dfx.createOrReplaceTempView("dfx")

analise_financas_query_fechamento=spark.sql("select xd.Setor, round(Sum(dfx.Open),2) as Abertura, round(Sum(dfx.Close),2) as Fechamento, round((Sum(dfx.Close)) - (Sum(dfx.Open)),2) as Diferenca from xd join dfx on xd.Codigo=dfx.Ticker group by xd.Setor order by Fechamento desc")

analise_financas_query_fechamento.show(10,False)

fechamento=analise_financas_query_fechamento.to_pandas_on_spark()
fechamento_analise=analise_financas_query_fechamento.toPandas()

def plot_coluna(coluna_nome): #criamos uma classefuncao para plotar cada coluna e reduzir o codigo
  fig,ax = plt.subplots(figsize=(35,12))

  ax.barh(fechamento_analise['Setor'].iloc[0:10],fechamento_analise[coluna_nome].iloc[0:10],color=["orange","red","blue","yellow","gray","pink","Brown","green","lime","purple"])
  ax.set_title(coluna_nome + " dos Setores",fontsize=18)

  for idx, val in enumerate(fechamento_analise[coluna_nome].iloc[0:10]):
      txt = val
      y_coord=idx-0.1
      x_coord=val+2
      ax.text(x=x_coord,y=y_coord,s=txt,fontsize=16)

  return plt.show()


plot_coluna("Fechamento")

plot_coluna("Abertura")

plot_coluna("Diferenca")

ticker_maior_fechamento=spark.sql("select distinct(Ticker),Setor from xd join dfx on xd.Codigo=dfx.Ticker where Setor="+"'"+str(fechamento_analise['Setor'][0])+"'")

ticker_maior_fechamento.show(10,False)


tickers_melhor_setor=ticker_maior_fechamento.to_pandas_on_spark()
tickers_melhor_setor_graphs=ticker_maior_fechamento.toPandas()


tickers_melhor_setor_graphs['Ticker']

ticket_list2=list((tickers_melhor_setor_graphs['Ticker']).iloc[0:10])
df2 = yf.download(ticket_list2,period="2y",group_by="ticker");

#Análise Exploratória

new_df2=df2.stack(1).reset_index().rename(columns={'level_1':'Ticker'})
new_dfx2 = pd.DataFrame(new_df)
dfx2=new_dfx.to_spark()
dfx2.show()

dfx2.createOrReplaceTempView("dfx2")

analise_ticket_exploratoria_query=spark.sql("select Ticker, round(Sum(`Adj Close`),2) as Adj_Close,round(Sum(Dividends),2) as Dividends,round(Sum(High),2) as High,round(Sum(Low),2) as Low,round(Sum(Open),2) as Open,round(Sum(`Stock Splits`),2) as Stock_Splits,round(Sum(Volume),2) as Volume from dfx2 group by Ticker")

analise_ticket_exploratoria_query.show(10,False)

analise_ticket_exploratoria=analise_ticket_exploratoria_query.to_pandas_on_spark()

def analisar_coluna(coluna_nome): #criamos uma classefuncao para analisar cada coluna e reduzir o codigo
        coluna = coluna_nome
        media = analise_ticket_exploratoria[coluna_nome].mean()
        mediana = analise_ticket_exploratoria[coluna_nome].median()
        primeiro_quartil = analise_ticket_exploratoria[coluna_nome].quantile(.25)
        segundo_quartil = analise_ticket_exploratoria[coluna_nome].quantile(.50)
        terceiro_quartil = analise_ticket_exploratoria[coluna_nome].quantile(.75)
        quarto_quartil = analise_ticket_exploratoria[coluna_nome].quantile(1.0)
        desvio_padrao = analise_ticket_exploratoria[coluna_nome].std()
        maximo = analise_ticket_exploratoria[coluna_nome].max()
        minimo = analise_ticket_exploratoria[coluna_nome].min()
        Analise = (coluna, media, mediana, primeiro_quartil, segundo_quartil, terceiro_quartil, quarto_quartil, desvio_padrao, maximo, minimo)
        return Analise

analise_adj_close=analisar_coluna("Adj_Close")
analise_dividends=analisar_coluna("Dividends")
analise_low=analisar_coluna("Low")
analise_open=analisar_coluna("Open")
analise_stock_splits=analisar_coluna("Stock_Splits")
analise_volume=analisar_coluna("Volume")

data = [(analise_adj_close), (analise_dividends), (analise_low),(analise_open),(analise_stock_splits),(analise_volume)]

columns = StructType([ \
StructField("coluna",StringType(),True), \
StructField("media",StringType(),True), \
StructField("mediana",StringType(),True), \
StructField("primeiro_quartil",StringType(),True), \
StructField("segundo_quartil", StringType(), True), \
StructField("terceiro_quartil", StringType(), True), \
StructField("quarto_quartil", StringType(), True), \
StructField("desvio_padrao", StringType(), True), \
StructField("maximo", StringType(), True), \
StructField("minimo", StringType(), True) \
  ])

spark2 = SparkSession.builder.master("local[*]").appName("AnalisExploratória").getOrCreate()
sdf = spark2.createDataFrame(data=data,schema = columns)

sdf.createOrReplaceTempView("sdf")

analise_exploratoria_query=spark.sql("select coluna as coluna, round(media,2) as media,mediana,primeiro_quartil,segundo_quartil,terceiro_quartil,round(cast(quarto_quartil as decimal),2) as quarto_quartil,round(desvio_padrao,2) as desvio_padrao,maximo,minimo from sdf")

analise_exploratoria_query.show(10,False)

# Predicao

def f_regressao_polinomial(x, y):
  modelo = np.poly1d(np.polyfit(x, y, 3))
  return modelo

for acao in ticket_list2:
  serie = df2[acao, 'Close']
  serie.plot()

plt.title("Cotação x tempo", fontsize = 25)
# Legendas
#plt.legend(loc='lower left')
#plt.legend(loc=2)
plt.legend(loc='best')
plt.rcParams["figure.figsize"] = (15,15)


plt.style.use('bmh')

df2_pred_list = df2[ticket_list2]


ticket_list2

len(ticket_list2)

df2_pred_list[ticket_list2[0]]

df2_pred_close_list = []

for i in range(len(ticket_list2)):
  df2_pred_close_list.append(df2_pred_list[ticket_list2[i]][["Close"]])

df2_pred_close_list

df2_pred_close_list[3]

x_list = []

y_list = []

x_treino_list = []

x_test_list = []

y_treino_list = []

y_test_list = []

model_list = []

x_futuro_list = []

dias_futuros = 30

for i in range(len(ticket_list2)): #trocando valores NaN pela media
  df2_pred_close_list[i][df2_pred_close_list[i]['Close'].isna()] = df2_pred_close_list[i].mean()

total = (sum([df2_pred_close_list[0], df2_pred_close_list[1],df2_pred_close_list[2],df2_pred_close_list[3],df2_pred_close_list[4],df2_pred_close_list[5],df2_pred_close_list[6],df2_pred_close_list[7],df2_pred_close_list[8],df2_pred_close_list[9]]))

total

df2_pred_close_list[3]

df2_pred_close_list[3].mean()

for i in range(len(ticket_list2)):
  df2_pred_close_list[i]["Predicao"] = df2_pred_close_list[i]["Close"].shift(-dias_futuros)

total["Predicao"] = total["Close"].shift(-dias_futuros)

total

df2_pred_close_list[3]

len(df2_pred_close_list[3])

len(df2_pred_close_list)

#for i in range(len(ticket_list2)): #removendo linhas com NaN
#  df2_pred_close_list[i] = df2_pred_close_list[i][~np.isnan(df2_pred_close_list[i]).any(axis=1)]

df2_pred_close_list[3]

total

for i in range(len(ticket_list2)):
  x_list.append(np.array(df2_pred_close_list[i].drop(["Predicao"], 1))[:-dias_futuros])
  y_list.append(np.array(df2_pred_close_list[i]["Predicao"])[:-dias_futuros])
  x_futuro_list.append(df2_pred_close_list[i].drop(["Predicao"], 1)[:-dias_futuros])
  x_futuro_list[i] = x_futuro_list[i].tail(dias_futuros)
  x_futuro_list[i] = np.array(x_futuro_list[i])

total_x = np.array(total.drop(["Predicao"], 1))[:-dias_futuros]
total_y = np.array(total["Predicao"])[:-dias_futuros]
total_x_futuro = total.drop(["Predicao"], 1)[:-dias_futuros]
total_x_futuro = total_x_futuro.tail(dias_futuros)
total_x_futuro = np.array(total_x_futuro)

def plot_predicao(i):
  x_treino, x_test ,y_treino, y_test = train_test_split(x_list[i], y_list[i], test_size = 0.25)
  model = LinearRegression().fit(x_treino, y_treino)
  predicao = model.predict(x_futuro_list[i])
  predicoes = predicao
  valido = df2_pred_close_list[i][x_list[i].shape[0]:]
  valido["Predicoes"] = predicoes
  plt.figure(figsize = (15, 15))
  plt.title("Valor da Ação "+ticket_list2[i]+" no Período")
  plt.xlabel("Período")
  plt.ylabel("Preço de Fechamento")
  plt.plot(df2_pred_close_list[i]["Close"])
  plt.plot(valido[["Close", "Predicoes"]])
  plt.legend(["Origem", "Real", "Predicao"])

for i in range(len(ticket_list2)):
  plot_predicao(i)
  
  

total_treino_x, total_test_x ,total_treino_y, total_test_y = train_test_split(total_x, total_y, test_size = 0.25)
model = LinearRegression().fit(total_treino_x, total_treino_y)
predicao = model.predict(total_x_futuro)
predicoes = predicao
valido = total[total_x.shape[0]:]
valido["Predicoes"] = predicoes
plt.figure(figsize = (15, 15))
plt.title("Valor da Ação "+"Total do Setor"+" no Período")
plt.xlabel("Período")
plt.ylabel("Preço de Fechamento")
plt.plot(total["Close"])
plt.plot(valido[["Close", "Predicoes"]])
plt.legend(["Origem", "Real", "Predicao"])

# CSVs

analise_financas_query_fechamento.toPandas().to_csv("Analise_Melhor_Setor.csv", index=False , sep=':')
files.download("Analise_Melhor_Setor.csv")

analise_exploratoria_query.toPandas().to_csv("Analise_Exploratoria.csv", index=False , sep=':')
files.download("Analise_Exploratoria.csv")
