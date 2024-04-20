from dask.distributed import Client
from prefect import task, Flow
from prefect.executors import LocalDaskExecutor
import time
import logging
from datetime import datetime

# Configurando logging
logging.basicConfig(level=logging.INFO)

def main():
    # Inicializa o cliente Dask
    client = Client()
    print(client)

    # Definir uma tarefa simples com log
    @task
    def my_task(x):
        start_time = datetime.now()
        time.sleep(1)  # Simular alguma computação
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info(f"Task {x} started at {start_time} and finished at {end_time}")
        logging.info(f"Task {x} duration: {duration} seconds")
        return x * 2

    # Inicia a medição do tempo de execução do fluxo
    flow_start_time = datetime.now()

    # Definir o fluxo
    with Flow("My Dask-Enabled Flow", executor=LocalDaskExecutor(scheduler="threads")) as flow:
        # Criar múltiplas tarefas para demonstrar execução paralela
        results = my_task.map([1, 2, 3, 4])

    # Executar o fluxo
    flow.run()

    # Calcula a duração total do fluxo
    flow_end_time = datetime.now()
    flow_duration = (flow_end_time - flow_start_time).total_seconds()
    print(f"Fluxo concluído em {flow_duration} segundos.")

    # Não há necessidade de manter o script em execução após a conclusão do fluxo
    # time.sleep(60)  # Essa linha pode ser removida se você não precisar do painel do Dask

if __name__ == "__main__":
    main()
