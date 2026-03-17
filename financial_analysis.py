# instalar as dependências
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop2.tgz 
!tar xf spark-3.3.0-bin-hadoop2.tgz

# configurar as variáveis de ambiente e o Spark
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.3.0-bin-hadoop2.tgz"

!pip install -q findspark

# tornar o pyspark "importável"
import findspark
findspark.init('spark-3.3.0-bin-hadoop2')

# iniciar uma sessão local
from pyspark.sql import SparkSession,SQLContext
import matplotlib.pyplot as plt

spark = SparkSession.builder.master("local[*]").appName("DadosBrazilianStockMarket").getOrCreate()

# Instalando o yahoo finance
!pip install yfinance --upgrade --no-cache-dir

import pyspark.pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf 
import requests



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

states5=xd.select('Codigo').toPandas()['Codigo']
states6=list(states5)
data=yf.Tickers(states6).history(period="10d").tail(30)

new_df=data.stack(1).reset_index().rename(columns={'level_1':'Ticker'})
new_dfx = pd.DataFrame(new_df)
dfx=new_dfx.to_spark()
dfx.show()

dfx.createOrReplaceTempView("dfx")

analise_financas_query=spark.sql("select xd.Setor, Sum(dfx.Open) as Abertura, Sum(dfx.Close) as Fechamento from xd join dfx on xd.Codigo=dfx.Ticker group by xd.Setor")

analise_financas_query.show(10,False)

#normalizando os dados para os gráficos comparativos
analise_financas_query_normalizacao=analise_financas_query.toPandas()
query_normalized_abertura=(analise_financas_query_normalizacao['Abertura'] - analise_financas_query_normalizacao['Abertura'].min()) / (analise_financas_query_normalizacao['Abertura'].max() - analise_financas_query_normalizacao['Abertura'].min())
query_normalized_fechamento=(analise_financas_query_normalizacao['Fechamento'] - analise_financas_query_normalizacao['Fechamento'].min()) / (analise_financas_query_normalizacao['Fechamento'].max() - analise_financas_query_normalizacao['Fechamento'].min())


analise_financas_query_normalizacao['Abertura']=query_normalized_abertura
analise_financas_query_normalizacao['Fechamento']=query_normalized_fechamento

import matplotlib.pyplot as plt

fig,ax = plt.subplots(figsize=(26,88))
  
ax.barh(analise_financas_query_normalizacao['Setor'],analise_financas_query_normalizacao['Fechamento'],color=["orange","red","blue","yellow","gray","pink","Brown","green","lime","purple"])
ax.set_title("Melhores Setores",fontsize=18)


for idx, val in enumerate(analise_financas_query_normalizacao['Fechamento']):
    txt = val
    y_coord=idx-0.1
    x_coord=val

    ax.text(x=x_coord,y=y_coord,s=txt,fontsize=16)

    plt.show()
