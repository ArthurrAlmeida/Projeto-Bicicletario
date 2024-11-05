# Projeto-Bicicletario 
<p> Durante o projeto eu usei apenas ferramentas presentes no AWS gratuito e durante o processo deixei aberto para o publico todo o projeto. Primeiro criei Buckets no S3 para armazenar os dados em CSV, depois criei Crawlers para que esses Buckets fossem atualizados e assim comecei a criação dos ETLs. Para criação dos ETLs usei a interface visual do Glue, nela podemos apenas selecionar os arquivos e com isso a ferramenta gera o código de ETL (que está presente aqui com o nome de ETLRox), onde por meio de Data Soucers fiz a conexão com os dados dos Buckets S3, depois fiz a transformação desses dados e por fim inseri tudo no Bucket de DataLake, para finalmente conseguir realizar as analises usando o Athenas, nele podemos fazer consultas usando SQL padrão (também está presente aqui com o nome de QUERYS) e com isso finalizado o processo.</p><p> Durante o projeto eu usei apenas ferramentas presentes no AWS gratuito e durante o processo deixei aberto para o publico todo o projeto. Primeiro criei Buckets no S3 para armazenar os dados em CSV, depois criei Crawlers para que esses Buckets fossem atualizados e assim comecei a criação dos ETLs. Para criação dos ETLs usei a interface visual do Glue, nela podemos apenas selecionar os arquivos e com isso a ferramenta gera o código de ETL (que está presente aqui com o nome de ETLRox), onde por meio de Data Soucers fiz a conexão com os dados dos Buckets S3, depois fiz a transformação desses dados e por fim inseri tudo no Bucket de DataLake, para finalmente conseguir realizar as analises usando o Athenas, nele podemos fazer consultas usando SQL padrão (também está presente aqui com o nome de QUERYS) e com isso finalizado o processo.</p>

## Desenho
![2](https://github.com/user-attachments/assets/01e9abbc-5893-47dc-a6c0-225cb5517348)


## Versões e ferramentas utilizadas
<p> AWS S3 </p>
<p> AWS Glue </p>
<p> AWS Athena </p>
<p> Python 3 </p>
<p> Scala 2 </p>
<p> Spark 3 </p>
<p> Glue 4 </p>



# Solução

<h3> Modelagem dos dados </h3>

![image](https://github.com/KnightAlmeida/Projeto-Bicicletario/assets/94095714/1c4ff1e3-6c79-4648-8d69-e7941c7a41d7)

<h3> ETL feito no Glue </h3>

![image](https://github.com/KnightAlmeida/Projeto-Bicicletario/assets/94095714/64795956-31e5-4be6-93d8-ab787d0d1163)


<h3> Analises Realizadas </h3>

<p> Query 1 </p>
<p>Escreva uma query que retorna a quantidade de linhas na tabela Sales.SalesOrderDetail pelo campo SalesOrderID, desde que tenham pelo menos três linhas de detalhes.</p>
<p> Query 2 </p>
<p>Escreva uma query que ligue as tabelas Sales.SalesOrderDetail, Sales.SpecialOfferProduct e Production.Product e retorne os 3 produtos (Name) mais vendidos (pela soma de OrderQty), agrupados pelo número de dias para manufatura (DaysToManufacture).</p>

<h3> Links para acessar o projeto </h3>

<p> Acessar ETL no Glue </p>
<p> https://sa-east-1.console.aws.amazon.com/gluestudio/home?region=sa-east-1#/editor/job/EtlRox/graph </p>
<p> Acessar Buckets no S3 </p>
<p> https://s3.console.aws.amazon.com/s3/buckets/datalake-rox-proj?region=sa-east-1&bucketType=general&tab=objects </p>
<p> Acessar consultas no Athenas </p>
<p> https://sa-east-1.console.aws.amazon.com/athena/home?region=sa-east-1#/query-editor </p>


<h4> Lembrando que para acessar os links acima é necessário conta na AWS </h4>

#

<h3> Considerações finais </h3>
<p> Bom pessoal, esse é meu primeiro projeto em Big Data usando tecnologias em nuvens, tudo nele foi feito com ferramentas gratuitas. Estou disponivel para conversar sobre, tirar dúvidas e aprender com outros Engenheiros de Dados. Obrigado pela atenção e por tirar um tempo para avaliar meu projeto. </p>











