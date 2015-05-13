#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <assert.h>

#define CHECK_ERROR(cmd) \
  if ((cmd) != 0) {perror(#cmd); exit(1);}


#define NCHECK_ERROR(cmd) \
  if ((cmd) == -1) {perror(#cmd); exit(1);}

int main(int argc, const char* argv[]) {
  if (argc != 5) {
    printf("Usage: %s mlocksize blocksize nrepeat filename\n", argv[0]);
    exit(1);
  }
  long mlocksize = atol(argv[1])*1024*1024;
  long blocksize = atol(argv[2]);
  long nrepeat = atol(argv[3]);
  const char* filename = argv[4];

  char* iobuf = (char*)aligned_alloc(blocksize*16, blocksize);
  void* mlockp = malloc(mlocksize);
  CHECK_ERROR(mlock(mlockp, mlocksize));
  int fd = open(filename, O_RDWR | O_DIRECT | O_DSYNC);
  NCHECK_ERROR(fd);

  struct stat buf;
  CHECK_ERROR(fstat(fd, &buf));
  off_t filesize = buf.st_size;
  printf("filesize = %lu\n", filesize);
  off_t nblock = filesize / blocksize;

  srandom(time(NULL));
  long totalns = 0;
  for (int i = 0; i < nrepeat; i++) {
    for (int j = 0; j < blocksize; j++) iobuf[j] = random();
    off_t start = ((unsigned long long)random()*(unsigned long long)random() % nblock) * blocksize;

    struct timespec start_time;
    struct timespec end_time;
    CHECK_ERROR(clock_gettime(CLOCK_MONOTONIC, &start_time));
    off_t off = lseek(fd, start, SEEK_SET);
    NCHECK_ERROR(off);
    NCHECK_ERROR(write(fd, iobuf, blocksize));
    CHECK_ERROR(clock_gettime(CLOCK_MONOTONIC, &end_time));
    long ns = (end_time.tv_sec - start_time.tv_sec) * 1000000000L + end_time.tv_nsec - start_time.tv_nsec;
    //printf("ns = %L, speed = %fMB per sec.\n", ns, blocksize * 1000.0 / ns);

    totalns += ns;
  }
  printf("\nTotal: ns = %u, write speed = %f MB per sec.\n", totalns, blocksize * 1000.0 * nrepeat / totalns);
  return 0;
}
