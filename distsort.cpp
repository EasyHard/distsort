#include <mpi.h>
#include <algorithm>
#include <stdio.h>
#include <stdlib.h>
#include "common.h"
typedef int KeyType;
#define KEY_MPITYPE MPI_INT
KeyType *locey;
int world_size;int world_rank;
int main(int argc, char** argv) {
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    int nchunk = atoi(argv[1]);int nitem = CHUNK_SIZE*nchunk/ITEM_SIZE;
    locey = new KeyType[nitem];
    char fname[1024]; sprintf(fname, "locey/locey.%d", world_rank);
    FILE* f = fopen(fname, "rb");
    fread(locey, sizeof(KeyType), nitem, f); fclose(f);

    FILE* outputf[nchunk];
    for (int i = 0; i < nchunk; i++) {
        char fname[1024]; sprintf(fname, "output/output.node%d.%d", world_rank, i);
        outputf[i] = fopen(fname, "wb");
    }
    unsigned char *buff = new unsigned char[CHUNK_SIZE];
    for (int i = 0; i < 1 /*nchunk*/; i++) {
        char fname[1024]; sprintf(fname, "data/data.node%d.%d", world_rank, i);
        FILE* f = fopen(fname, "rb"); fread(buff, 1, CHUNK_SIZE, f); fclose(f);
        for (int i = 0; i < CHUNK_SIZE/ITEM_SIZE; i++) {
            KeyType *k = (KeyType*)(buff+i*ITEM_SIZE); int dest = *k / (nchunk*CHUNK_SIZE/ITEM_SIZE);
            // printf("node %d: k = %d, dest = %d\n", world_rank, *k, dest);
            MPI_Request request;
            MPI_Isend(buff+i*ITEM_SIZE, ITEM_SIZE, MPI_CHAR, dest, 0, MPI_COMM_WORLD, &request);

            if (i % (256) == 255) {
                printf("node %d: i = %d\n", world_rank, i);
                while (true) {
                    MPI_Status status; int incoming;
                    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &incoming, &status);
                    if (!incoming) break;
                    char item[ITEM_SIZE];
                    MPI_Recv(item, ITEM_SIZE, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);
                    KeyType *k = (KeyType*)item; int idx = *k % (nchunk*CHUNK_SIZE);
                    int cidx = idx / CHUNK_SIZE; int offset = idx % CHUNK_SIZE;
                    //printf("node %d got msg, locey: %d, cidx = %d, offset = %d\n", world_rank, *k, cidx, offset);
                    fseek(outputf[cidx], offset, SEEK_SET); fwrite(item, 1, ITEM_SIZE, outputf[cidx]);
                }
            }

        }

    }
    for (int i = 0; i < nchunk; i++) fclose(outputf[i]);
    MPI_Finalize();
}
