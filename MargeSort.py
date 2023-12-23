import csv
import os
import tempfile

def load_data(file_path):
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)  # Se houver cabeçalho
        data = [row for row in reader]
    return header, data

def sort_runs(data, buffer_size):
    temp_files = []
    num_files = 0
    while data:
        chunk = data[:buffer_size]
        chunk.sort(key=lambda x: x[0])  # Substituir pelo índice da chave de ordenação
        temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', encoding='utf-8')
        temp_files.append(temp_file.name)
        with open(temp_file.name, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(chunk)
        data = data[buffer_size:]
        num_files += 1
    return temp_files, num_files

def merge_runs(files):
    if len(files) == 1:
        return files[0]

    merged_files = []
    while len(files) > 1:
        new_files = []
        for i in range(0, len(files), 2):
            if i + 1 < len(files):
                merged_file = merge(files[i], files[i + 1])
                merged_files.append(merged_file)
            else:
                new_files.append(files[i])
        files = merged_files + new_files
        merged_files = []
    return files[0]

def merge(file1, file2):
    with open(file1, 'r', newline='', encoding='utf-8') as f1, \
         open(file2, 'r', newline='', encoding='utf-8') as f2, \
         tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', encoding='utf-8') as temp:
        
        reader1 = csv.reader(f1)
        reader2 = csv.reader(f2)
        writer = csv.writer(temp)

        row1 = next(reader1, None)
        row2 = next(reader2, None)

        while row1 and row2:
            if row1[0] <= row2[0]:  # Comparação com base na chave de ordenação
                writer.writerow(row1)
                row1 = next(reader1, None)
            else:
                writer.writerow(row2)
                row2 = next(reader2, None)

        while row1:
            writer.writerow(row1)
            row1 = next(reader1, None)
        while row2:
            writer.writerow(row2)
            row2 = next(reader2, None)

    return temp.name

def display_menu():
    print("\nMenu:")
    print("1. Carregar arquivo")
    print("2. Dividir em 'runs'")
    print("3. Mesclar 'runs'")
    print("4. Sair")
    choice = input("Escolha uma opção: ")
    return choice

def main():
    file_loaded = False
    file_path = None
    buffer_size = 1000  # Tamanho do buffer para cada "run"

    while True:
        choice = display_menu()

        if choice == '1':
            file_path = input("Digite o caminho do arquivo CSV: ")
            header, data = load_data(file_path)
            file_loaded = True
            print("Arquivo carregado com sucesso!")

        elif choice == '2':
            if file_loaded:
                temp_files, num_files = sort_runs(data, buffer_size)
                print(f"Arquivo dividido em {num_files} 'runs'")
            else:
                print("Por favor, carregue o arquivo primeiro.")

        elif choice == '3':
            if file_loaded:
                if num_files > 1:
                    final_sorted_file = merge_runs(temp_files)
                    print(f"Arquivos 'runs' mesclados em {final_sorted_file}")
                    for temp_file in temp_files:
                        os.remove(temp_file)
                else:
                    print("Nada para mesclar. O arquivo já está ordenado.")
            else:
                print("Por favor, carregue o arquivo primeiro.")

        elif choice == '4':
            print("Saindo do programa...")
            break

        else:
            print("Opção inválida. Escolha novamente.")

if __name__ == "__main__":
    main()
