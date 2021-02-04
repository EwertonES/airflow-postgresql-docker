# Apache/Airflow + PostgreSQL + Docker

Crie o webserver do Airflow com PostgreSQL usando Docker em um ambiente Linux.

## Máquina de testes (GCP)
**Machine type:** e2-small (2 vCPUs, 2 GB memory)

**Zone:** us-central1-a

**Image:** 	ubuntu-minimal-2010-groovy-v20210129

**Size:** 10 GB

**Price:** $12.63/month

## Pré-instalação

Entrar no modo sudo
```sh 
sudo -i
```
Update package lists
```sh 
apt-get update
```
Instalar git
```sh 
apt-get install git
```
Instalar docker
```sh 
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh
```
Instalar docker-compose
```sh 
curl -L "https://github.com/docker/compose/releases/download/1.28.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-composed
```
Baixar repositório do Github
```sh 
git clone https://github.com/EwertonES/airflow-postgresql-docker.git
```

## Instalação

Com o terminal dentro do diretório do Gitlab, construa a imagem do Apache/Airflow usando o arquivo Dockerfile:
```sh 
docker build -t apache/airflow:dockerfile .
```
Puxe a imagem do PostgreSQL do Docker Hub:
```sh 
docker pull postgres
```
Dê permissões ao arquivo "entrypoint.sh":
```sh 
chmod 777 -R .
```
Construa os contêineres com o arquivo do docker-compose:
```sh 
docker-compose up
```
O Airflow poderá ser acessado por meio da URL: **https://localhost:8080/**

Com o Google Cloud, lembrar de abrir a porta no Firewall e usar o IP externo.
