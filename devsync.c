#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <assert.h>


/*#define debug(...) fprintf (stderr, __VA_ARGS__) */
#define debug(...) 

#ifndef O_LARGEFILE
#define O_LARGEFILE 0
#endif

void fail(const char *errmsg) 
{
  perror(errmsg);
  exit(1);
}

static int srcfd;
int open_src(const char *path)
{
  return((srcfd = open(path, O_RDONLY|O_LARGEFILE))<0);
}

static int dstfd;
int open_dst(const char *path)
{
  return((dstfd = open(path, O_RDWR|O_CREAT|O_LARGEFILE, 0666))<0);
}

#define SBLOCK_BSHIFT 20
#define SBLOCK_SIZE (1<<SBLOCK_BSHIFT)
#define SBLOCK_MASK (SBLOCK_SIZE-1)
#define BLOCK_BSHIFT 12
#define BLOCK_SIZE (1<<BLOCK_BSHIFT)
#define BLOCK_DIFF_BSHIFT (SBLOCK_BSHIFT - BLOCK_BSHIFT)
#define BLOCK_DIFF_MASK ((1<<BLOCK_DIFF_BSHIFT)-1)

static off_t bytes_written, extent_start, extent_length;
static void *extent_buf;

void flush_extent(void)
{
  if(extent_length) {
    debug("flush_extent: %lu blocks @ %lu\n", extent_length, extent_start);
    if((BLOCK_SIZE * extent_length) != pwrite(dstfd, extent_buf, (extent_length *BLOCK_SIZE), extent_start * BLOCK_SIZE)) {
      fail("Failed write call to destination");
    }
    extent_length = 0;
  }
}




static unsigned char srcbuf[SBLOCK_SIZE];
void *read_src(off_t block_number)
{
  static off_t last_sblock = -1, sblock, blocks;
  off_t idx, bytes_read;
  sblock = block_number >> BLOCK_DIFF_BSHIFT;
  
  idx = block_number & BLOCK_DIFF_MASK;
  if(sblock != last_sblock)
  {
    flush_extent();
    debug("read_src(%lu): reading sblock %lu: ", block_number, sblock);
    bytes_read = pread(srcfd, srcbuf, SBLOCK_SIZE, (off_t)sblock * SBLOCK_SIZE);
    debug("read %lu bytes\n", bytes_read);
    if(bytes_read <= 0)
      return(NULL);
    if(bytes_read % BLOCK_SIZE)
      fail("aborted: source is not a multiple of of BLOCK_SIZE!");
    blocks = bytes_read >> BLOCK_BSHIFT;
    last_sblock = sblock;
  }
  if(idx >= blocks)
    return(NULL);
  
  return((void *) srcbuf + (idx * BLOCK_SIZE));
}



static unsigned char dstbuf[SBLOCK_SIZE];
void *read_dst(off_t block_number)
{
  static off_t last_sblock = -1, sblock, blocks;
  off_t idx, bytes_read;
  sblock = block_number >> BLOCK_DIFF_BSHIFT;
  
  idx = block_number & BLOCK_DIFF_MASK;
  if(sblock != last_sblock)
  {
    debug("read_dst(%lu): reading sblock %lu: ", block_number, sblock);
    bytes_read = pread(dstfd, dstbuf, SBLOCK_SIZE, (off_t)sblock * SBLOCK_SIZE);
    debug("read %lu bytes\n", bytes_read);
    if(bytes_read <= 0)
      return(NULL);
    if(bytes_read % BLOCK_SIZE)
      fail("aborted: source is not a multiple of of BLOCK_SIZE!");
    blocks = bytes_read >> BLOCK_BSHIFT;
    last_sblock = sblock;
  }
  if(idx >= blocks)
    return(NULL);  

  return((void *) dstbuf + (idx * BLOCK_SIZE));
}


void *write_dst(off_t block_number, void *buf)
{
  debug("write_dst(%lu) ", block_number);

  if(extent_length && block_number == (extent_start + extent_length)) {
    extent_length++;
  }
  else {
    if(extent_length)
      flush_extent();
    extent_start = block_number;
    extent_buf = buf;
    extent_length = 1;
  }
    
/*  if(BLOCK_SIZE != pwrite(dstfd, buf, BLOCK_SIZE, block_number * BLOCK_SIZE))
  {
    fail("Failed write call to destination");
  }*/
  bytes_written += BLOCK_SIZE;
}



main(int argc, char *argv[])
{
  off_t n = 0;
  void *src, *dst;
  time_t update=0;
  
  if(argc < 3) {
    fail("Usage: devsync /dev/source /path/to/destination.file");
  }

  if(open_src(argv[1]))
    fail(argv[1]);

  if(open_dst(argv[2]))
    fail(argv[2]);
  
  while((src = read_src(n)) && (dst = read_dst(n)))
  {
    if(memcmp(src, dst, BLOCK_SIZE)) {
      write_dst(n, src);
    }
    n++;

    if(update != time(NULL)) {
      update = time(NULL);
      printf("Read %luMB / Wrote %luMB  \r", n * BLOCK_SIZE >> 20, bytes_written >> 20);
      fflush(stdout);
    }
  }
  flush_extent();
  fprintf(stderr, "Finished: End of %s after %luMB.  ", src ? "destination" : "source", n * BLOCK_SIZE >> 20);
  fprintf(stderr, "Wrote %luMB.\n", bytes_written >> 20);
  return(0);
}


