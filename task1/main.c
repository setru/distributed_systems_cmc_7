#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#define MAX_SLEEP_TIME 10
const char *critical_file = "critical.txt";

#include "mpi.h"
#include "queue.h"

#define START_ENTER 0
#define FINISH_ENTER 1
#define ALLOW_ENTER 2

#define DIM 4

void critical_section(MPI_Comm comm, int rank) 
{
    int fd;
    if (!access(critical_file, F_OK)) {

		printf("Ошибка: файл %s существовал во время обработки критической секции %d\n", critical_file, rank);
		MPI_Abort(comm, MPI_ERR_FILE_EXISTS);

	} else {

		fd = open(critical_file, O_CREAT, S_IRWXU);
		if (!fd)
		{
			printf("Ошибка: невозможно создать файл %s процессом %d\n", critical_file, rank);
			MPI_Abort(comm, MPI_ERR_FILE);
		}

		int time_to_sleep = random() % MAX_SLEEP_TIME;
		sleep(time_to_sleep);

		if (close(fd))
		{
			printf("Ошибка: не удалось закрыть файл %s процессом %d\n", critical_file, rank);
			MPI_Abort(comm, MPI_ERR_FILE);
		}

		if (remove(critical_file))
		{
			printf("Ошибка: не удалось удалить файл %s процессом %d\n", critical_file, rank);
			MPI_Abort(comm, MPI_ERR_FILE);
		}
	}
}

int main(int argc, char **argv)
{
	MPI_Init(&argc, &argv);
	
	srand(time(NULL));
    MPI_Comm comm;
	int rank, send2master_rank, send2proc_rank, master_rank;
    int coords[2], send2master_coords[2], send2proc_coords[2], master_coords[2] = {0};
    int buf;
    MPI_Status status;
    MPI_Request req;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); // определяем ранк процесса

    int dims[2] = {DIM, DIM};
    int periodic[2] = {0};
    MPI_Cart_create(MPI_COMM_WORLD, 2, dims, periodic, 0, &comm); // создание транспьютерной матрицы
    MPI_Cart_coords(comm, rank, 2, coords); // получаем координаты текущего процесса в транспьютерной матрице
    MPI_Cart_rank(comm, master_coords, &master_rank); // получаем rank координатора


    if (rank != master_rank) {
        int num_rcvs = ((coords[0] == 0) ? DIM * (DIM - coords[1]) : DIM - coords[0]) - 1;
        if (coords[0] != 0) {
            send2master_coords[0] = coords[0] - 1;
            send2master_coords[1] = coords[1];
        } else {
            send2master_coords[0] = coords[0];
            send2master_coords[1] = coords[1] - 1;
        }
        MPI_Cart_rank(comm, send2master_coords, &send2master_rank);
        buf = rank;
        MPI_Send(&buf, 1, MPI_INT, send2master_rank, START_ENTER, comm);
        printf("Отправлен запрос на разрешение на вход для (%d, %d)\n", coords[0], coords[1]);
        for (int i = 0; i < 3*num_rcvs + 1; i++) {
            MPI_Recv(&buf, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, comm, &status);
            if (status.MPI_TAG == START_ENTER) {
                MPI_Send(&buf, 1, MPI_INT, send2master_rank, START_ENTER, comm);
            } else if (status.MPI_TAG == ALLOW_ENTER) {
                if (buf == rank) {
                    printf("Получено разрешение на вход для (%d, %d)\n", coords[0], coords[1]); 
                    critical_section(comm, rank);
                    MPI_Send(&buf, 1, MPI_INT, send2master_rank, FINISH_ENTER, comm);   
                } else {
                    MPI_Cart_coords(comm, buf, 2, send2proc_coords);
                    if (send2proc_coords[1] > coords[1]) {
                        send2proc_coords[0] = coords[0];
                        send2proc_coords[1] = coords[1] + 1;
                    } else {
                        send2proc_coords[0] = coords[0] + 1;
                        send2proc_coords[1] = coords[1];
                    }
                    MPI_Cart_rank(comm, send2proc_coords, &send2proc_rank);
                    MPI_Send(&buf, 1, MPI_INT, send2proc_rank, ALLOW_ENTER, comm);
                }
            } else if (status.MPI_TAG == FINISH_ENTER) {
                MPI_Send(&buf, 1, MPI_INT, send2master_rank, FINISH_ENTER, comm);
            }
        }
    } else { // coordinator actions
        for (int i = 0; i < 2 * SIZE; ++i) {
            MPI_Recv(&buf, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, comm, &status);
            if (status.MPI_TAG == START_ENTER) {
                if (is_free) {
                    is_free = 0;
                    MPI_Cart_coords(comm, buf, 2, coords);
                    printf("Разрешен вход для (%d, %d)\n", coords[0], coords[1]);
                    if (coords[1] > 0) {
                        coords[0] = 0;
                        coords[1] = 1;
                    } else {
                        coords[0] = 1;
                        coords[1] = 0;
                    }
                    MPI_Cart_rank(comm, coords, &rank);
                    MPI_Isend(&buf, 1, MPI_INT, rank, ALLOW_ENTER, comm, &req);
                } else {
                    enQueue(buf);
                }
            } else if (status.MPI_TAG == FINISH_ENTER) {
                is_free = 1;
                MPI_Cart_coords(comm, buf, 2, coords);
                printf("Завершена обработка критической секции для (%d, %d)\n", coords[0], coords[1]);
                int next_proc = deQueue();
                if (next_proc != -1) {
                    is_free = 0;
                    MPI_Cart_coords(comm, next_proc, 2, coords);
                    printf("Разрешен вход для (%d, %d)\n", coords[0], coords[1]);
                    if (coords[1] > 0) {
                        coords[0] = 0;
                        coords[1] = 1;
                    } else {
                        coords[0] = 1;
                        coords[1] = 0;
                    }
                    MPI_Cart_rank(comm, coords, &rank);
                    MPI_Isend(&next_proc, 1, MPI_INT, rank, ALLOW_ENTER, comm, &req);
                }
            }
        }
        critical_section(comm, master_rank);
        printf("Завершена обработка критической секции для координатора\n");
    }
    MPI_Finalize();
    return 0;
}