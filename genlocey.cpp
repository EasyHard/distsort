// mpirun <name> <k> <shuffle round>
#include <mpi.h>
#include <algorithm>
#include <stdio.h>
#include <stdlib.h>

typedef int KeyType;
#define KEY_MPITYPE MPI_INT

KeyType *sendbuf, *recvbuf;
int world_size;int world_rank;
int main(int argc, char** argv) {
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    int k = atoi(argv[1]); int r = atoi(argv[2]);
    sendbuf = new KeyType[k*world_size]; recvbuf = new KeyType[k*world_size];
    for (int i = 0; i < k*world_size; i++) sendbuf[i] = world_rank*world_size*k+i;
    for (int t = 0; t < r; t++) {
        printf("round %d\n", t);
        std::random_shuffle(sendbuf, sendbuf+k*world_size);
        MPI_Alltoall(sendbuf, k, KEY_MPITYPE, recvbuf, k, KEY_MPITYPE, MPI_COMM_WORLD);
        KeyType* tmp = recvbuf; recvbuf = sendbuf; sendbuf = tmp;
    }

    // printf("node %d:", world_rank);
    // for (int i = 0; i < world_size*k; i++) printf("%d ", recvbuf[i]); printf("\n");

    char fname[1024]; sprintf(fname, "locey/locey.%d", world_rank);
    FILE* f = fopen(fname, "wb");
    fwrite(recvbuf, sizeof(KeyType), world_size*k, f);
    fclose(f);
    MPI_Finalize();
}
