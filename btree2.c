#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>

typedef uint32_t pgno_t;
typedef uint32_t flag_t;
typedef unsigned char BYTE;
typedef unsigned long ULONG;

//// ===========================================================================
////                              tmp tmp tmp tmp tmp
/* ;;: remove -- for debugging */
/*
  bp(X) where X is false will raise a SIGTRAP. If the process is being run
  inside a debugger, this can be caught and ignored. It's equivalent to a
  breakpoint. If run without a debugger, it will dump core, like an assert
*/
#if defined(__i386__) || defined(__x86_64__)
#define bp(x) do { if(!(x)) __asm__ volatile("int $3"); } while (0)
#elif defined(__thumb__)
#define bp(x) do { if(!(x)) __asm__ volatile(".inst 0xde01"); } while (0)
#elif defined(__aarch64__)
#define bp(x) do { if(!(x)) __asm__ volatile(".inst 0xd4200000"); } while (0)
#elif defined(__arm__)
#define bp(x) do { if(!(x)) __asm__ volatile(".inst 0xe7f001f0"); } while (0)
#else
STATIC_ASSERT(0, "debugger break instruction unimplemented");
#endif


#define __packed        __attribute__((__packed__))
#define UNUSED(x) ((void)(x))

#ifdef DEBUG
# define DPRINTF(fmt, ...)                                              \
        fprintf(stderr, "%s:%d " fmt "\n", __func__, __LINE__, __VA_ARGS__)
#else
# define DPRINTF(fmt, ...)	((void) 0)
#endif
#define DPUTS(arg)	DPRINTF("%s", arg)
#define TRACE(...) DPUTS("")

#define BT_SUCC 0
#define SUCC(x) ((x) == BT_SUCC)


#define BT_MAPADDR  ((void*)0x10000) /* ;;: change constant */

#define BT_PAGEBITS 14ULL
#define BT_PAGEWORD 32ULL
#define BT_PAGESIZE (1ULL << BT_PAGEBITS) /* 16K */
#define BT_ADDRSIZE (BT_PAGESIZE << BT_PAGEWORD)
#define FO2BY(fo) ((fo) << BT_PAGEBITS)



//// ===========================================================================
////                                  btree types

/*
  btree page header. all pages share this header. Though for normal pages (not
  meta and not root) it will be zeroed out
*/
typedef struct BT_pageheader BT_pageheader;
struct BT_pageheader {
  uint32_t ver;                 /* version ;;: todo. make a decision. we can unconditionally store this in all pages or simply store it in BT_metadata. Same goes for the rest of the values */
  uint32_t chk;                 /* checksum */
  uint32_t blk_base[8];         /* block base array for striped node partition */
  uint8_t  blk_cnt;             /* currently highest valid block base */
  uint8_t  depth;               /* tree depth */
#define BP_DIRTY 0x01           /* for exclusive use by root page */
#define BP_META  0x02
/* #define	BP_BRANCH	 0x04 */
/* #define	BP_LEAF		 0x08 */
  uint8_t  flags;
  uint8_t  _pad0;
  uint8_t  dirty[256];          /* dirty bit map */
  uint32_t  _pad1;
} __packed;

/*
  btree key/value data format
*/
typedef struct BT_kv BT_kv;
struct BT_kv {
  uint32_t va;                  /* virtual address offset */
  pgno_t   fo;                  /* file offset */
} __packed;

/*
   all pages in the memory arena consist of a header and data section
*/
#define BT_DAT_MAXBYTES (BT_PAGESIZE - sizeof(BT_pageheader))
#define BT_DAT_MAXKEYS  (BT_DAT_MAXBYTES / sizeof(BT_kv))
typedef struct BT_page BT_page;
struct BT_page {
  BT_pageheader head;
  union {                       /* data section */
    BT_kv dat[BT_DAT_MAXKEYS];  /* va:fo pairs */
    BYTE  datc[0];              /* byte-level view */
  };
};
static_assert(sizeof(BT_page) == BT_PAGESIZE);
static_assert(BT_DAT_MAXBYTES % sizeof(BT_kv) == 0);

#define BT_MAGIC   0xBADDBABE
#define BT_VERSION 1
/*
   a meta page is like any other page, but the data section is used to store
   additional information
*/
typedef struct BT_meta BT_meta;
struct BT_meta {
  uint32_t  magic;
  uint32_t  version;
  pgno_t    last_pg;            /* last page used in file */
  uint32_t  _pad0;
  uint64_t  txnid;
  void     *fix_addr;           /* fixed addr of btree */
  pgno_t    root;
} __packed;
static_assert(sizeof(BT_meta) <= BT_DAT_MAXBYTES);

/* macro to access the metadata stored in a page's data section */
#define METADATA(p) ((BT_meta *)(void *)(p)->datc)

typedef struct BT_state BT_state;
struct BT_state {
  uint16_t      flags;
  int           data_fd;
  int           meta_fd;
  char         *path;
  ULONG         branch_page_cnt;
  ULONG         leaf_page_cnt;
  ULONG         depth;
  void         *fixaddr;
  BYTE         *map;
  BT_meta      *meta_pages[2];  /* double buffered */
  unsigned int  toggle;         /* which double-buffered db are we using? */
};



//// ===========================================================================
////                            btree internal routines


#define CLOSE_FD(fd)                            \
  do {                                          \
    close(fd);                                  \
    fd = -1;                                    \
  } while(0)

static int
_bt_state_read_header(BT_state *state, BT_meta *meta)
{
  /* TODO: actually read the header and copy the data to meta when we implement
     persistence */
  BYTE bytes[BT_PAGESIZE];
  int len;

  TRACE();

  len = pread(state->data_fd, bytes, BT_PAGESIZE, 0);

  /* TODO: since we haven't implemented persistence yet, a persistent file
     shouldn't exist. When we do implement persistence, conditionally parse the
     metadata if it exists */
  assert(errno = ENOENT);
  assert(len == 0);

  return errno;
}

static int
_bt_state_meta_new(BT_state *state, BT_meta *meta)
{
  BT_page *p1, *p2;
  int rc, pagesize;

  TRACE();

  pagesize = sizeof *p1 * 2;

  /* initialize meta struct */
  meta->magic = BT_MAGIC;
  meta->version = BT_VERSION;
  meta->last_pg = 1;
  meta->txnid = 0;
  meta->fix_addr = BT_MAPADDR;
  meta->root = UINT32_MAX;      /* ;;: actually should probably be 0 */

  /* initialize the metapages */
  p1 = calloc(2, BT_PAGESIZE);
  p2 = p1 + 1;
  p2->head.flags = p1->head.flags = BP_META;
  p2->head.blk_cnt = p1->head.blk_cnt = 1;
  p2->head.depth = p1->head.depth = 1;

  /* copy the metadata into the metapages */
  memcpy(METADATA(p1), meta, sizeof *meta);
  memcpy(METADATA(p2), meta, sizeof *meta);

  /* sync new meta pages with disk */
  rc = write(state->data_fd, p1, pagesize * 2);

  free(p1);
  if (rc == pagesize * 2)
    return BT_SUCC;
  else
    return errno;
}

static int
_bt_state_meta_which(BT_state *state, int *which)
{
  /* TODO */
  *which = 0;
  return BT_SUCC;
}

static int
_bt_state_load(BT_state *state)
{
  int rc, which;
  int new = 0;
  BT_meta meta = {0};
  BT_page *p = 0;
  
  TRACE();

  if (!SUCC(rc = _bt_state_read_header(state, &meta))) {
    if (rc != ENOENT) return rc;
    DPUTS("creating new db");
    new = 1;
  }

  state->map = mmap(BT_MAPADDR,
                    BT_ADDRSIZE,
                    PROT_READ,
                    MAP_FIXED | MAP_SHARED,
                    state->data_fd,
                    0);

  /* new db, so populate metadata */
  if (new) {
    if (!SUCC(rc = _bt_state_meta_new(state, &meta))) {
      munmap(state->map, BT_ADDRSIZE);
      return rc;
    }
  }

  p = (BT_page *)state->map;
  state->meta_pages[0] = METADATA(p);
  state->meta_pages[1] = METADATA(p + 1);

  if (!SUCC(rc = _bt_state_meta_which(state, &which)))
    return rc;

  return BT_SUCC;
}


//// ===========================================================================
////                            btree external routines

int
bt_state_new(BT_state **state)
{
  TRACE();

  BT_state *s = calloc(1, sizeof *s);
  s->meta_fd = s->data_fd = -1;
  s->fixaddr = BT_MAPADDR;
  *state = s;
  return BT_SUCC;
}

#define DATANAME "/data.pma"
int
bt_state_open(BT_state *state, const char *path, ULONG flags, mode_t mode)
{
  /*
    since we aren't implementing persistence for now, we should just init a
    fresh BT_state

    TODO: read the data file and restore the persistent state
  */
  int oflags, rc;
  char *dpath;

  TRACE();
  UNUSED(flags);
  UNUSED(path);                 /* TODO use path */

  oflags = O_RDWR | O_CREAT;
  dpath = malloc(strlen(path) + sizeof(DATANAME));
  if (!dpath) return ENOMEM;
  sprintf(dpath, "%s" DATANAME, path);

  if ((state->data_fd = open(path, oflags, mode)) == -1)
    return errno;

  if (!SUCC(rc = _bt_state_load(state)))
    goto e;

  oflags |= O_DSYNC;            /* see man 2 open */
  if ((state->meta_fd = open(dpath, oflags, mode)) == -1) {
    rc = errno;
    goto e;
  }

  state->path = strdup(path);

 e:
  /* cleanup FDs stored in state if anything failed */
  if (!SUCC(rc)) {
    if (state->data_fd != -1) CLOSE_FD(state->data_fd);
    if (state->meta_fd != -1) CLOSE_FD(state->meta_fd);
  }

  free(dpath);
  return rc;
}

int main(int argc, char *argv[])
{
  BT_state *state;
  bt_state_new(&state);
  bt_state_open(state, "./pmatest", 0, 644);
  return 0;
}
