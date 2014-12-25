#include <mpi.h>
#include <algorithm>
#include <stdio.h>
#include <stdlib.h>
#include "common.h"
typedef int KeyType;
#define KEY_MPITYPE MPI_INT
KeyType *locey;
int world_size;int world_rank;
#define NPACK (128) // number of items in a package
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
    unsigned char *pack = new unsigned char[world_size*NPACK*ITEM_SIZE];
    unsigned char *recvpack = new unsigned char[NPACK*ITEM_SIZE];
    int *count = new int[world_size]; memset(count, 0, sizeof(int)*world_size);
    int *tcount = new int[world_size]; memset(tcount, 0, sizeof(int)*world_size);
    int nrecv = 0;
    for (int i = 0; i < nchunk; i++) {
        char fname[1024]; sprintf(fname, "data/data.node%d.%d", world_rank, i);
        FILE* f = fopen(fname, "rb"); fread(buff, 1, CHUNK_SIZE, f); fclose(f);
        for (int i = 0; i < CHUNK_SIZE/ITEM_SIZE; i++) {
            KeyType *k = (KeyType*)(buff+i*ITEM_SIZE); int dest = *k / (nchunk*(CHUNK_SIZE/ITEM_SIZE));
            // printf("node %d: k = %d, dest = %d\n", world_rank, *k, dest);
            memcpy(pack+dest*NPACK*ITEM_SIZE+count[dest]*ITEM_SIZE, buff+i*ITEM_SIZE, ITEM_SIZE);
            count[dest]++;
            if (count[dest] == NPACK) {
                MPI_Request request; tcount[dest]++;
                MPI_Isend(pack+dest*NPACK*ITEM_SIZE, ITEM_SIZE*NPACK, MPI_CHAR, dest, 0, MPI_COMM_WORLD, &request);
                count[dest] = 0;
            }
            while (true) {
                MPI_Status status; int incoming;
                MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &incoming, &status);
                if (!incoming) break; nrecv++;
                MPI_Recv(recvpack, ITEM_SIZE*NPACK, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);
                for (int i = 0; i< NPACK; i++) {
                    unsigned char *item = recvpack+ITEM_SIZE*i;
                    KeyType *k = (KeyType*)item; int idx = *k % (nchunk*(CHUNK_SIZE/ITEM_SIZE));
                    int cidx = idx / (CHUNK_SIZE/ITEM_SIZE); int offset = idx % (CHUNK_SIZE/ITEM_SIZE);
                    //printf("node %d got msg, locey: %d, cidx = %d, offset = %d\n", world_rank, *k, cidx, offset);
                    fseek(outputf[cidx], offset*ITEM_SIZE, SEEK_SET); fwrite(item, 1, ITEM_SIZE, outputf[cidx]);
                }
            }
        }

    }

    while (true) {
        MPI_Status status; int incoming;
        MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &incoming, &status);
        if (!incoming) break; nrecv++;
        MPI_Recv(recvpack, ITEM_SIZE*NPACK, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);
        for (int i = 0; i< NPACK; i++) {
            unsigned char *item = recvpack+ITEM_SIZE*i;
            KeyType *k = (KeyType*)item; int idx = *k % (nchunk*(CHUNK_SIZE/ITEM_SIZE));
            int cidx = idx / (CHUNK_SIZE/ITEM_SIZE); int offset = idx % (CHUNK_SIZE/ITEM_SIZE);
            //printf("node %d got msg, locey: %d, cidx = %d, offset = %d\n", world_rank, *k, cidx, offset);
            fseek(outputf[cidx], offset*ITEM_SIZE, SEEK_SET); fwrite(item, 1, ITEM_SIZE, outputf[cidx]);
        }
    }
    
    for (int i = 0; i < nchunk; i++) fclose(outputf[i]);
    printf("node %d: nrecv = %d\n", world_rank, nrecv);
    for (int i = 0; i < world_size; i++) printf("node %d: tcount[%d] = %d\n", world_rank, i, tcount[i]);
    MPI_Finalize();
}
