## Projeto integrador 3: Grupo 3
Machine Learning applied to Sports - Web scraping/Web Crawler/Search Engine

### Instruções
 - Crie um novo ambiente e o ative
 - Faça a instalação dos módulos necessários: ``` pip install requirements.txt ``` 
 - Execute o script: ``` web_scaper.py [ano_inicial] [ano_final] ```
   - Exemplo:  ``` web_scaper.py 2005 2010 ```
   - Opções extras:
     - ``` -o ```
       - exporta dados da temporada
     - ``` -ts ```
       - exporta dados dos jogos dos times
     - ``` -stat ```
       - exporta nomes e descrições das colunas
     - ``` -pickle ```
       - exporta os dados em formato .pickle
     - ``` -w=[n] ```
       - específica o número de <i>workers</i> que o script irá utilizar (padrão = 4)
       - exemplo: ``` -w=6 ```
       - Mais <i>workers</i> significa menos tempo de execução, porém mais consumo de memória e processamento.
       - Para máquinas com 8GB de RAM, utilize no máximo <b>6</b>.

 - Pasta destino padrão: ```./data/```
 
Informações recolhidas do site https://www.pro-football-reference.com/.
