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
#define PMA_FAIL
#define SUCC(x) ((x) == BT_SUCC)

#define FREE_DBI 0
#define MAIN_DBI 1

#define BT_PAGEBITS 14ULL
#define BT_PAGEWORD 32ULL
#define BT_PAGESIZE (1ULL << BT_PAGEBITS) /* 16K */
#define BT_ADDRSIZE (BT_PAGESIZE << BT_PAGEWORD)

#define FO2BY(fo) ((fo) << BT_PAGEBITS)

#define BT_MAPADDR  ((void*)0x10000)

#define F_ISSET(w, f)	 (((w) & (f)) == (f))
#define MINIMUM(a,b)	 ((a) < (b) ? (a) : (b))
#define IS_LEAF(p)	 F_ISSET((p)->bp_flag, BP_LEAF)
#define IS_BRANCH(p)	 F_ISSET((p)->bp_flag, BP_BRANCH)


typedef struct BT_pageheader BT_pageheader;
struct BT_pageheader {          /* a page of storage */
  pgno_t pgno;
#define	BP_BRANCH	 0x01   /* branch page */
#define	BP_LEAF		 0x02   /* leaf page */
#define BP_META          0x04   /* meta page */
#define BP_DIRTY         0x08   /* dirty page */
  flag_t flag;
  uint16_t numkeys;
} __packed;

typedef struct BT_kv BT_kv;
struct BT_kv {
  /* ;;: va can be made a va OFFSET if we store the base fixed address in the
       btree metadata. The logic is slightly simpler now just storing the raw
       addr. */
  uint64_t va;                  /* virtual address */
  pgno_t   fo;                  /* file offset */
} __packed;

typedef struct BT_page BT_page;
struct BT_page {
#define bp_pgno bp_bph.pgno
#define bp_flag bp_bph.flag
#define bp_numkeys bp_bph.numkeys
  BT_pageheader bp_bph;         /* header */
  union {                       /* data */
    BT_kv dat[0];
    BYTE  datc[BT_PAGESIZE - sizeof(BT_pageheader)];
  };                            /* ;;: -- should maybe be an AOS - store keys
                                     and values in separate arrays -- doesn't
                                     matter at all though if we store 32 bit va
                                     offsets instead (planned). So will leave
                                     as-is */
} __packed;

static_assert(sizeof(BT_page) == BT_PAGESIZE);

typedef struct BT_db BT_db;
struct BT_db {
  uint16_t flags;
  uint16_t depth;
  ULONG    branch_pages;
  ULONG    leaf_pages;
  ULONG    entries;
  pgno_t   root;
  time_t   created_at;
};

#define BT_MAGIC   0xBADDBABE
#define BT_VERSION 1
typedef struct BT_meta BT_meta;
struct BT_meta {
  uint32_t magic;
  uint32_t version;
  /* uint32_t flags; */
  BT_db    dbs[2];              /* double buffering -- WRONG. you should just
                                   have one BT_db */
  pgno_t   last_pg;             /* last page used in file */
  ULONG    txnid;
};

/* metadata is stored in the data section of a normal page */
#define METADATA(p) ((void *)(p)->datc)

/* much the same functionality as MDB_env and MDB_db structs except we don't
   need to store multiple databases */
typedef struct BT_state BT_state;
struct BT_state {
  uint16_t      flags;
  int           data_fd;
  /* int lock_fd; */
  int           meta_fd;        /* ;;: lmdb has a duplicate fd of the file at data_fd with O_DSYNC flag */
  char         *path;
  ULONG         branch_page_cnt;
  ULONG         leaf_page_cnt;
  void         *fixaddr;
  BYTE         *map;
  BT_meta      *meta_pages[2];  /* double buffered */
  unsigned int  toggle;         /* which double-buffered db are we using? */
  unsigned int  maxdbs;         /* 2 */
  unsigned int  numdbs;
  BT_db        *dbs[2];
};


//// ===========================================================================
////                               internal routines

#define CLOSE_FD(fd)                            \
  do {                                          \
    close(fd);                                  \
    fd = -1;                                    \
  } while(0)


static int
_bt_open_dfile(const char *path, ULONG oflags, mode_t mode) {
  int fd;

  if ((fd = open(path, oflags, mode)) == -1)
    return errno;

  /* using a sparse file for persistence for now. can change this later */
  ftruncate(fd, BT_ADDRSIZE);   /* idempotent */
  return fd;
}

static int
_bt_state_read_header(BT_state *state, BT_meta *meta)
{
  BYTE bytes[BT_PAGESIZE];
  BT_page *p;
  BT_meta *m;
  int len, i;

  if ((len = pread(state->data_fd, bytes, BT_PAGESIZE, 0)) != BT_PAGESIZE) {
    DPRINTF("strange page: %s", strerror(errno));
    return errno;
  }

  /* a new db is zero-filled */
  for (i = 0; i < BT_PAGESIZE; i++) {
    if (bytes[i] != 0) break;
  }

  if (i != BT_PAGESIZE)
    return ENOENT;              /* nothing else to do here */

  /* otherwise, validate metadata page */
  p = (BT_page *)bytes;
  if (p->bp_flag & BP_META) {
    DPRINTF("page %lu missing meta page flag", p->bp_pgno);
    return EINVAL;
  }

  m = METADATA(p);
  /* validate magic */
  if (m->magic != BT_MAGIC) {
    DPRINTF("metapage inconsistent magic: %lu", m->magic);
    return EINVAL;
  }

  /* check metapage version matches binary version */
  if (m->version != BT_VERSION) {
    DPRINTF("version mismatch. metapage: %u, binary: %u",
            m->version, BT_VERSION);
    return EINVAL;
  }

  /* validated. persist to passed meta pointer */
  memcpy(meta, m, sizeof *m);
  return BT_SUCC;
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
  meta->dbs[0].root = meta->dbs[1].root = ~0;

  /* initialize the metapages */
  p1 = calloc(2, BT_PAGESIZE);
  p2 = p1 + 1;
  p1->bp_pgno = 0;
  p2->bp_pgno = 1;
  p2->bp_pgno = p1->bp_flag = BP_META;

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
/*
  determine the latest metadata page to use and stores the index in which
 */
{
  if (state->meta_pages[0]->txnid < state->meta_pages[1]->txnid)
    *which = 1;
  else
    *which = 0;

  DPRINTF("Latest metadata page is %d", latest);
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
    /* ENOENT expected if we're creating anew. Anything else is not */
    if (rc != ENOENT) return rc;
    DPUTS("new db");
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


  DPUTS("opened database");
  DPRINTF("version: %u", state->meta_pages[which]->version);
  DPRINTF("txnid  : %ul", state->meta_pages[which]->txnid);
  DPRINTF("created at: %llu", state->meta_pages[0]->dbs[MAIN_DBI].created_at);
  DPRINTF("depth: %u", state->meta_pages[0]->dbs[MAIN_DBI].depth);
  DPRINTF("entries: %lu", state->meta_pages[0]->dbs[MAIN_DBI].entries);
  DPRINTF("root: %lu", state->meta_pages[0]->dbs[MAIN_DBI].root);

  return BT_SUCC;
}

//// ===========================================================================
////                              wip wip wip wip wip

/*

  ;;: requires more thought. How do we know the bounds of the last BT_kv in
    page->dat again? The parent's key merely gives us X <= MAX(page->dat) but
    not the upper bound of the MAX(page->dat) interval

    -- should probably implement the basic btree finding/inserting/splitting
       routines first...

*/

static int
_f1(BT_state *state, BT_page *page)
{
  uint16_t n;

  assert(page->bp_flag & BP_LEAF);

  if ((n = page->bp_numkeys) == 0)
    return BT_SUCC;

  BT_kv *kv = page->dat;
  for(; n < 0; kv++, n--) {
    uint64_t va = kv->va;
    pgno_t   fo = kv->fo;
    /* calculate len */
    size_t len = 0;
    mmap((void*)(uint64_t)va,
         len,
         PROT_READ | PROT_WRITE,
         MAP_FIXED | MAP_SHARED,
         state->data_fd,
         0);
  }

  return 255;
}

static int
_f2(BT_state *state, BT_page *page)
{
  uint64_t n;
  assert(page->bp_flag & BP_BRANCH);

  if ((n = page->bp_numkeys) == 0)
    return BT_SUCC;

  BT_kv *kv = page->dat;
  for(; n < 0; kv++, n--) {
    uint64_t va = kv->va;
    pgno_t   fo = kv->fo;
    BT_page *child = (void *)&state->map[FO2BY(fo)];

    if (IS_LEAF(child))
      _f1(state, child);
    else
      _f2(state, child);
  }

  return 255;
}

int
_pma_restore_map(BT_state *state)
{
  /* trace the btree and restore the mmap configuration */
}



//// ===========================================================================
////                               external routines

/*
  ;;: note, the external "bt" prefixed routines aren't intended to be used by
  ares. "pma" prefixed routines are. the bt routines may implement btree
  loading/saving/finding/splitting/txn_begin/txn_commit/syncing etc. routines
  etc. pma prefixed routines will orchestrate these. In some cases
  trivially. e.g. pma_sync should just be able to call bt_sync. however,
  something like pma_load may make several bt calls and internal _pma
  calls. e.g. bt_state_new, bt_state_open, _pma_restore_map, ... Currently just
  using one file for ease. But _pma and _bt routines can be split out into
  separate headers.
*/

int
bt_state_new(BT_state **state)
{
  TRACE();

  BT_state *s = calloc(1, sizeof *state);
  s->data_fd = -1;
  s->meta_fd = -1;
  s->fixaddr = BT_MAPADDR;
  s->maxdbs = 2;
  *state = s;
  return BT_SUCC;
}

#define DATANAME "/data.pma"
int
bt_state_open(BT_state *state, const char *path, ULONG flags, mode_t mode)
/* ;;: flags currently unused. Later could use it to open db in readonly
     mode. RW hard-coded rn */
{
  int oflags, rc;
  char *dpath;

  TRACE();

  UNUSED(flags);
  oflags = O_RDWR | O_CREAT;

  dpath = malloc(strlen(path) + sizeof(DATANAME));
  if (!dpath) return ENOMEM;
  sprintf(dpath, "%s" DATANAME, path);

  if ((state->data_fd = _bt_open_dfile(dpath, flags, mode)) == -1) {
    return errno;
  }

  if ((rc = _bt_state_load(state)) != BT_SUCC) {
    goto e;
  }

  oflags |= O_DSYNC;            /* see man 2 open */
  if ((state->meta_fd = open(dpath, oflags, mode)) == -1) {
    rc = errno;
    goto e;
  }

  state->path = strdup(path);
  state->dbs[0] = calloc(state->maxdbs, sizeof *state->dbs);
  state->dbs[1] = calloc(state->maxdbs, sizeof *state->dbs);
  state->numdbs = 2;

 e:
  /* cleanup state FDs if anything failed */
  if (!SUCC(rc)) {
    if (state->data_fd != -1) CLOSE_FD(state->data_fd);
    if (state->meta_fd != -1) CLOSE_FD(state->meta_fd);
  }

  free(dpath);
  return rc;
}

void
bt_state_close(BT_state *state)
{
  if (state == 0)
    return;

  /* ;;: todo complete this after additions to state -- sync, free the mallocs
       etc. */
  close(state->data_fd);
  close(state->meta_fd);
  free(state);
}

int main(int argc, char *argv[])
{
  /* printf("0x%zx, 0x%zx", sizeof(BT_page), BT_PAGESIZE); */

  /* ;;: temporary -- for testing purposes */
  BT_state *state;
  bt_state_new(&state);
  bt_state_open(state, "./pmatest", 0, 644);
  _pma_restore_map(state);
  return 0;
}
