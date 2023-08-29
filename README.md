# Aws-S3-Data-Read

Creation of two python scripts for browsing and reading S3 buckets and files.
Navigation is done through numeric menus and can be accessed via command prompt.
After selecting the desired file, the script generates dataframes from json, parquet, csv and avro files contained in folders and subfolders of the selected bucket.
There is 2 types of scripts: The first one use secret key and access key to read data from aws. 
The second one don't use keys to access data, but you need a previous connection with AWS CLI, and then you can read data.

[PT]
Criação de dois scripts python para navegação e leitura de buckets e arquivos S3.
A navegação é feita através de menus numéricos e pode ser acessada via prompt de comando. 
Após selecionar o arquivo desejado, o script gera dataframes a partir de arquivos json, parquet, csv e avro contidos em pastas e subpastas do bucket selecionado.
Neste repositório existem 2 tipos de scripts: O primeiro usa chave secreta e chave de acesso para acessar a AWS. 
O segundo não usa chaves para acessar dados, mas você precisa de uma conexão prévia com AWS CLI, para então poder ler os dados.
