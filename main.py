import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count

# Tamanho do vetor e configuração de threads/processos
vetor = np.random.randint(1, 100, size=1_000_000)  # Gerando um vetor grande com 1.000.000 de elementos
num_processos = cpu_count()  # Usar o número de CPUs disponíveis

# Convertendo o vetor para um DataFrame do pandas
df = pd.DataFrame(vetor, columns=['valores'])

# Função para somar um pedaço do DataFrame
def soma_subvetor(df_parte):
    return df_parte['valores'].sum()

# Dividir o DataFrame em sub-dataframes com base no número de processos
sub_dataframes = np.array_split(df, num_processos)

# Soma paralela usando multiprocessing
if __name__ == "__main__":
    with Pool(processes=num_processos) as pool:
        # Executa a função soma_subvetor em cada parte do DataFrame em paralelo
        resultados = pool.map(soma_subvetor, sub_dataframes)
        
    # Soma dos resultados parciais
    soma_total = sum(resultados)
    print("Soma total:", soma_total)
