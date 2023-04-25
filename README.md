# Processo de ETL e Análise para a empresa fictícia Varejo Max. 

Quem é a VarejoMax? 

É uma empresa de varejo referência em produtos de qualidade máxima aos seus clientes. Oferece serviços de varejo físico ou online e uma ampla variedade de produtos, desde cosméticos até roupas e acessórios. Além de um excelente atendimento ao cliente, muitas promoções e descontos exclusivos

Levantou-se vários problemas de negócios para que pudessem ser melhorados e assim, conseguir melhorar as vendas. Então, o tomador de decisão solicitou algumas perguntas a serem respondidas. Tais como:

Especificação para construção dos dashboard
VENDAS = quantidade de itens multiplicados pelo valor do item.

- O acumulado de vendas do último ano por Região e País. Ele gostaria de ter essa visão através de um Mapa Mundial diretamente no Relatório.
- Quantidade de vendas dos últimos 10 dias através de um gráfico de colunas.
- Quantidade de vendas e a Quantidade acumulada de vendas dos últimos 30 dias.
- Uma visão acumulada das vendas do último ano por Canal e País. De forma que seja possível ver a distribuição das vendas um determinado país por canal.
- Vale a pena construir outras filiais para melhorar o atendimento aos clientes? 

De forma resumida, neste projeto, foram realizadas as seguintes etapas: extração dos dados no formato CSV, tratamento com a linguagem PySpark e análises utilizando a biblioteca Pandas. Os dados foram armazenados no Cloud Storage, processados pelo BigQuery e, por fim, utilizados para criar os dashboards no Looker Studio.


Dashboard no Looker Studio no Google Cloud Plataform
![Pagina1](https://user-images.githubusercontent.com/87997775/234148990-10005a74-b4fe-41b5-aa77-44e7f5670424.png)
![Pagina_2](https://user-images.githubusercontent.com/87997775/234148996-491761ee-7eb7-4bbf-a9ee-f01a25c85430.png)
![Pagina_3](https://user-images.githubusercontent.com/87997775/234149006-dacfba38-e2db-4af3-9982-7a590b29187a.png)


