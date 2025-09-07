from collections import defaultdict

# Função Map: Emite ((cidade, data), (temperatura, 1)) para cada registro
def map_temperatures(record):
    """
    Processa um registro e emite uma tupla (chave, valor).
    A chave é uma tupla (cidade, data) e o valor é (temperatura, 1).
    """
    timestamp, city, temp = record

    # Extrai apenas a parte da data do timestamp (ex: '2025-09-01')
    date = timestamp.split(" ")[0]
    key = (city, date)
    value = (temp, 1)

    return (key, value)

# Função Reduce: Calcula a temperatura média para a chave (cidade, data)
def reduce_temperatures(key, values):
    """
    Soma as temperaturas e as contagens para calcular a média de uma chave (cidade, data).
    """
    total_temp = 0
    total_count = 0
    for temp, count in values:
        total_temp += temp
        total_count += count

    avg_temp = total_temp / total_count if total_count > 0 else 0

    # Retorna a chave original (cidade, data) com sua temperatura média calculada
    return (key, avg_temp)

# Simulação de MapReduce
def mapreduce_avg_temp_per_day(data):
    """
    Executa as fases de Map, Shuffle e Reduce para a média diária.
    """
    # Fase Map
    mapped = [map_temperatures(record) for record in data]

    # Fase Shuffle/Sort: Agrupa os valores pela nova chave (cidade, data)
    grouped = defaultdict(list)
    for key, value in mapped:
        grouped[key].append(value)

    # Fase Reduce
    results = [reduce_temperatures(key, values) for key, values in grouped.items()]

    # Ordena os resultados primeiro por cidade, depois por data
    return sorted(results)

if __name__ == "__main__":

    input_file = "data/city_temperature.csv"

    try:
        with open(input_file, "r", encoding="utf-8") as content:
            next(content) # Pula a linha do cabeçalho
            text_data = content.readlines()

    except FileNotFoundError:
        print(f"Erro: O arquivo '{input_file}' não foi encontrado.")
        exit()

    # Processa os dados do CSV
    temp_data = []
    for line in text_data:
        try:
            datetime, city, temp = line.strip().split(",")
            temp_data.append((datetime, city, float(temp)))

        except ValueError:
            continue # Ignora linhas mal formatadas

    # Executa o trabalho de MapReduce
    result = mapreduce_avg_temp_per_day(temp_data)
    
    # Imprime os resultados formatados
    print("Temperatura Média por Cidade (Mês Completo):")
    for city, avg_temp in result:
        print(f"  - {city}: {avg_temp:.1f}°C")