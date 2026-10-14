#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <time.h>

#define BUFFER_SIZE 5       // Tamaño máximo del buffer
#define NUM_PRODUCERS 3     // Número de hilos productores (P0, P1, P2)
#define NUM_CONSUMERS 3     // Número de hilos consumidores (C0, C1, C2)
#define ITEMS_TO_PRODUCE 4  // Elementos a producir por cada productor

// Buffer circular compartido
int buffer[BUFFER_SIZE];
int in = 0;   // Índice donde el productor inserta
int out = 0;  // Índice donde el consumidor extrae

// Semáforos definidos en la Figura 5.13 de Stallings
sem_t s; // Semáforo binario de exclusión mutua (mutex)
sem_t n; // Semáforo contador: número de elementos en el buffer (full_slots)
sem_t e; // Semáforo contador: número de espacios libres en el buffer (empty_slots)

// Estructura para pasar el ID a cada hilo
typedef struct {
    int id;
} thread_data_t;

/**
 * Función que ejecutan los hilos productores (P0, P1, P2)
 */
void *producer(void *arg) {
    thread_data_t *data = (thread_data_t *)arg;
    int id = data->id;

    for (int item = 0; item < ITEMS_TO_PRODUCE; item++) {
        // 1. Etapa de Producción (fuera de la sección crítica)
        printf("[P%d] Producing %d ...\n", id, item);
        fflush(stdout);
        usleep((rand() % 100 + 50) * 1000); // Simula tiempo de producción

        sem_wait(&e); // Decrementa espacios libres (espera si el buffer está lleno)
        sem_wait(&s); // Entra a la sección crítica (exclusión mutua)

        // 2. Insertar elemento en el buffer (Añadir)
        buffer[in] = item;
        in = (in + 1) % BUFFER_SIZE;

        sem_post(&s); // Sale de la sección crítica
        sem_post(&n); // Incrementa la cantidad de elementos disponibles
    }

    pthread_exit(NULL);
}

/**
 * Función que ejecutan los hilos consumidores (C0, C1, C2)
 */
void *consumer(void *arg) {
    thread_data_t *data = (thread_data_t *)arg;
    int id = data->id;

    while (1) {
        sem_wait(&n); // Decrementa ítems disponibles (espera si el buffer está vacío)
        sem_wait(&s); // Entra a la sección crítica (exclusión mutua)

        // 1. Extraer elemento del buffer
        int item = buffer[out];
        out = (out + 1) % BUFFER_SIZE;

        sem_post(&s); // Sale de la sección crítica
        sem_post(&e); // Incrementa la cantidad de espacios libres

        // 2. Etapa de Consumo (fuera de la sección crítica)
        printf("------> [C%d] consumed %d\n", id, item);
        fflush(stdout);
        usleep((rand() % 100 + 50) * 1000); // Simula tiempo de consumo
    }

    pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
    srand(time(NULL));

    pthread_t producers[NUM_PRODUCERS];
    pthread_t consumers[NUM_CONSUMERS];
    thread_data_t p_data[NUM_PRODUCERS];
    thread_data_t c_data[NUM_CONSUMERS];

    // Inicialización de semáforos
    sem_init(&s, 0, 1);           // s = 1 (mutex)
    sem_init(&n, 0, 0);           // n = 0 (ítems disponibles)
    sem_init(&e, 0, BUFFER_SIZE); // e = BUFFER_SIZE (espacios libres)

    // Crear hilos consumidores
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        c_data[i].id = i;
        pthread_create(&consumers[i], NULL, consumer, &c_data[i]);
    }

    // Crear hilos productores
    for (int i = 0; i < NUM_PRODUCERS; i++) {
        p_data[i].id = i;
        pthread_create(&producers[i], NULL, producer, &p_data[i]);
    }

    // Esperar a que todos los productores terminen
    for (int i = 0; i < NUM_PRODUCERS; i++) {
        pthread_join(producers[i], NULL);
    }

    // Pausa breve para permitir que los consumidores procesen los últimos datos
    sleep(2);

    // Destruir semáforos
    sem_destroy(&s);
    sem_destroy(&n);
    sem_destroy(&e);

    printf("\n--- Ejecución completada exitosamente ---\n");
    return 0;
}