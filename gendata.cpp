// mpirun <name> <number of chunk> <shuffle round>
#include <mpi.h>
#include <algorithm>
#include <stdio.h>
#include <stdlib.h>
#include "common.h"

typedef int KeyType;
#define KEY_MPITYPE MPI_INT
KeyType *sendbuf, *recvbuf, *locey;

int world_size;int world_rank;
int main(int argc, char** argv) {
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    int nchunk = atoi(argv[1]); int r = atoi(argv[2]);
    int k = CHUNK_SIZE*nchunk/ITEM_SIZE/world_size;
    sendbuf = new KeyType[k*world_size]; recvbuf = new KeyType[k*world_size];
    for (int i = 0; i < k*world_size; i++) sendbuf[i] = world_rank*world_size*k+i;
    for (int t = 0; t < r; t++) {
        printf("round %d\n", t);
        std::random_shuffle(sendbuf, sendbuf+k*world_size);
        MPI_Alltoall(sendbuf, k, KEY_MPITYPE, recvbuf, k, KEY_MPITYPE, MPI_COMM_WORLD);
        KeyType* tmp = recvbuf; recvbuf = sendbuf; sendbuf = tmp;
    }
    char fname[1024]; sprintf(fname, "locey/locey.%d", world_rank);
    FILE* f = fopen(fname, "wb"); fwrite(recvbuf, sizeof(KeyType), world_size*k, f);
    fclose(f); locey = recvbuf;
    unsigned char *buff = new unsigned char[CHUNK_SIZE];
    for (int i = 0; i < nchunk; i++) {
        memset(buff, world_rank, CHUNK_SIZE);
        char fname[1024]; sprintf(fname, "data/data.node%d.%d", world_rank, i);
        for (int j = 0; j < CHUNK_SIZE/ITEM_SIZE; j++) {
            KeyType* k = (KeyType*)(buff+j*ITEM_SIZE);
            *k = locey[i*(CHUNK_SIZE/ITEM_SIZE)+j];
        }
        FILE* f = fopen(fname, "wb"); fwrite(buff, 1, CHUNK_SIZE, f); fclose(f);
    }

    MPI_Finalize();
}
