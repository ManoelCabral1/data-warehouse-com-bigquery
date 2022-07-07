# Data warehouse no Bigquery com SQL puro

## O que é BigQuery?
É uma solução em formato de estrutura para armazenamento de dados, criada pelo Google, com o objetivo de simplificar a integração e análise de um grande volume de informações (Big Data), tornando, por exemplo, mais assertivo, barato e produtivo o processo de levantamento, que possibilita aos líderes das empresas entender o resultado de seus investimentos.

Por ser um produto Google, possui uma ótima estrutura de processamento de dados, garantindo que o sistema de pesquisa SQL faça buscas muito mais rápidas!

Pensando nessa estrutrura simples e rápida de processamento do Bigquery, me desafiei a criar um Data warehouse usando somente SQL puro, para um conjunto de dados simples armazenados no Cloud Storage (o data lake do Google Cloud). Escolhi um dataset disponível no Kaggle com dados de vendas de games.

Por ser um conjunto de dados simples a identificação das dimensões e métricas é rápida.

### Modelagem dimensional
![modelagem domensional dw](https://github.com/ManoelCabral1/Prints/blob/main/modelagem-dw-games.png)

### Script de criação da área de stage e carga das tabelas dimensão

```
#Criação e das tabelas de stage e carga nas tabelas dimensão
create or replace table `stage_test.st_plataforma` as (
   select distinct Platform from `stage_test.st_data_base`
);

create or replace table `dw_test.dim_plataforma` as(
  select GENERATE_UUID() as ID,
  Platform from `stage_test.st_plataforma` 
);


create or replace table `stage_test.st_ano` as (
   select distinct Year from `stage_test.st_data_base`
);

create or replace table `dw_test.dim_ano` as(
  select GENERATE_UUID() as ID,
  Year from `stage_test.st_ano`
);

create or replace table `stage_test.st_genero` as (
   select distinct Genre from `stage_test.st_data_base`
);

create or replace table `dw_test.dim_genero` as(
  select GENERATE_UUID() as ID,
  Genre from `stage_test.st_genero`
);

create or replace table `stage_test.st_publicador` as (
   select distinct Publisher from `stage_test.st_data_base`
);

create or replace table `dw_test.dim_publicador` as(
  select GENERATE_UUID() as ID,
  Publisher from `stage_test.st_publicador`
);

 create or replace table `stage_test.st_game` as (
   select distinct Name from `stage_test.st_data_base`
);

create or replace table `dw_test.dim_game` as(
  select GENERATE_UUID() as ID,
  Name from `stage_test.st_game`
);
```

### Script de carga na tabela fato

```
#Carga completa da tabela fato
create or replace table `dw_test.fato_games_vendas` as(
select g.ID as game_ID,
       p.ID as plataforma_ID,
       y.ID as ano_ID,
       ge.ID as genero_ID,
       pu.ID as publicador_ID,
       b.Rank, 
       b.NA_Sales,
       b.EU_Sales,
       b.JP_Sales,
       b.Other_Sales,
       b.Global_Sales
        from `datawarehouse-test-354819.stage_test.st_data_base` as b
        inner join `dw_test.dim_plataforma` as p on (p.Platform = b.Platform)
        inner join `dw_test.dim_game` as g on (g.Name = b.Name)
        inner join `dw_test.dim_ano` as y on (y.Year = b.Year)
        inner join `dw_test.dim_genero` as ge on (ge.Genre = b.Genre)
        inner join `dw_test.dim_publicador` as pu on (pu.Publisher = b.Publisher));
```
### Script de carga incremental nas tabela dimensão

```
#carga incremental dim_plataforma
insert  `dw_test.dim_plataforma` 
  select GENERATE_UUID() as ID,
  Platform from `stage_test.st_plataforma` where Platform not in (select Platform from `dw_test.dim_plataforma`);

#carga incremental dim_ano
insert  `dw_test.dim_ano`
  select GENERATE_UUID() as ID,
  Year from `stage_test.st_ano` where Year not in (select Year from `dw_test.dim_ano`);

#carga incremental dim_game
insert  `dw_test.dim_game`
  select GENERATE_UUID() as ID,
  Name from `stage_test.st_game` where Name not in (select Name from `dw_test.dim_ano`);

#carga incremental dim_genero
insert  `dw_test.dim_genero`
  select GENERATE_UUID() as ID,
  Genre from `stage_test.st_genero` where Genre not in (select Genre from `dw_test.dim_ano`);

#carga incremental dim_publicador
insert  `dw_test.dim_publicador`
  select GENERATE_UUID() as ID,
  Publisher from `stage_test.st_publicador` where Publisher not in (select Publisher from `dw_test.dim_ano`);
```
Agora o DW está pronto para consultas para resolver problemas de negócios, a forma mais simples para essas consultas é criar views que serão disponibilizadas para os analistas de BI criarem relatórios, como nos exemplos abaixo.

### consulta vendas de games por ano

```
select y.Year as ano,
       sum(v.NA_Sales) as vendas_NA,
       sum(v.EU_Sales) as vendas_EU,
       sum(v.JP_Sales) as vendas_JP,
       sum(v.Other_Sales) vendas_Other,
       sum(v.Global_Sales) vendas_global
       from `dw_test.fato_games_vendas` as v
       inner join `dw_test.dim_ano` as y on (y.ID = v.ano_ID)

       group by 1
       order by ano
```
### consulta vendas de games por pubblicador

```
select  p.Publisher as publicador,
        g.Genre as genero,
        sum(f.NA_Sales) as vendas_NA,
        sum(f.JP_Sales) as vendas_JP_Sales,
        sum(f.Other_Sales) as vendas_Other_Sales,
        sum(f.Global_Sales) as vendas_global
        from `dw_test.fato_games_vendas` as f
        inner join `dw_test.dim_publicador` as p on ( p.ID = f.publicador_ID)
        inner join `dw_test.dim_genero` as g on (g.ID = f.genero_ID)

        group by 1,2
        order by 1
```
Essas consultas podem ser acessadas por vários aplicativos de BI como: PowerBI, DataStudio, ou nas planilhas Google, ou acessados via python ou outra linguagem, desde que se trenha as credenciais de acesso do Bigquery.

### Exemplo de relatório no Google Datastudio com os dado do DW.
![relatório](C:\Users\EBMquintto\Pictures\relatorio-dw.png)

