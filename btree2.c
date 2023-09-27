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

typedef uint32_t pgno_t;        /* a page number */
typedef uint32_t vaof_t;        /* a virtual address offset */
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

#define ZERO(s, n) memset((s), 0, (n))

#define S7(A, B, C, D, E, F, G) A##B##C##D##E##F##G
#define S6(A, B, C, D, E, F, ...) S7(A, B, C, D, E, F, __VA_ARGS__)
#define S5(A, B, C, D, E, ...) S6(A, B, C, D, E, __VA_ARGS__)
#define S4(A, B, C, D, ...) S5(A, B, C, D, __VA_ARGS__)
#define S3(A, B, C, ...) S4(A, B, C, __VA_ARGS__)
#define S2(A, B, ...) S3(A, B, __VA_ARGS__)
#define S(A, ...) S2(A, __VA_ARGS__)

#define KBYTES(x) ((size_t)(x) << 10)
#define MBYTES(x) ((size_t)(x) << 20)
#define GBYTES(x) ((size_t)(x) << 30)
#define TBYTES(x) ((size_t)(x) << 40)
#define PBYTES(x) ((size_t)(x) << 50)


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


#define BT_MAPADDR  ((void*) S(0x2000,0000,0000))

#define BT_PAGEBITS 14ULL
#define BT_PAGEWORD 32ULL
#define BT_PAGESIZE (1ULL << BT_PAGEBITS) /* 16K */
#define BT_ADDRSIZE (BT_PAGESIZE << BT_PAGEWORD)
/*
  FO2BY: file offset to byte
  get byte INDEX into pma map from file offset
*/
#define FO2BY(fo)                               \
  ((uint64_t)(fo) << BT_PAGEBITS)

/*
  BY2FO: byte to file offset
  get pgno from byte INDEX into pma map
*/
#define BY2FO(p)                                \
  ((pgno_t)((p) >> BT_PAGEBITS))

/*
  FO2PA: file offset to page
  get a reference to a BT_page from a file offset
*/
#define FO2PA(map, fo)                          \
  ((BT_page *)&(map)[FO2BY(fo)])


//// ===========================================================================
////                                  btree types

/*
  btree page header. all pages share this header. Though for metapages, you can
  expect it to be zeroed out.
*/
typedef struct BT_pageheader BT_pageheader;
struct BT_pageheader {
  uint8_t  dirty[256];          /* dirty bit map */
} __packed;

/*
  btree key/value data format

/*
  BT_dat is used to provide a view of the data section in a BT_page where data is
  stored like:
        va  fo  va  fo
  bytes 0   4   8   12

  The convenience macros given an index into the data array do the following:
  BT_dat_lo(i) returns ith   va (low addr)
  BT_dat_hi(i) returns i+1th va (high addr)
  BT_dat_fo(i) returns ith file offset
*/
typedef union BT_dat BT_dat;
union BT_dat {
  vaof_t va;                    /* virtual address offset */
  pgno_t fo;                    /* file offset */
};

/* like BT_dat but when a struct is more useful than a union */
typedef struct BT_kv BT_kv;
struct BT_kv {
  vaof_t va;
  pgno_t fo;
};

/* ;;: todo, perhaps rather than an index, return the data directly and typecast?? */
#define BT_dat_lo(i) ((i) * 2)
#define BT_dat_fo(i) ((i) * 2 + 1)
#define BT_dat_hi(i) ((i) * 2 + 2)

#define BT_dat_lo2(I, dat)
#define BT_dat_fo2(I, dat)
#define BT_dat_hi2(I, dat)

/* BT_dat_maxva: pointer to highest va in page data section */
#define BT_dat_maxva(p)                         \
  ((void *)&(p)->datd[BT_dat_lo(BT_DAT_MAXKEYS)])

/* BT_dat_maxfo: pointer to highest fo in page data section */
#define BT_dat_maxfo(p)                         \
  ((void *)&(p)->datd[BT_dat_fo(BT_DAT_MAXVALS)])

#define BT_DAT_MAXBYTES (BT_PAGESIZE - sizeof(BT_pageheader))
#define BT_DAT_MAXENTRIES  (BT_DAT_MAXBYTES / sizeof(BT_dat))
#define BT_DAT_MAXKEYS (BT_DAT_MAXENTRIES / 2)
#define BT_DAT_MAXVALS BT_DAT_MAXKEYS
static_assert(BT_DAT_MAXENTRIES % 2 == 0);

/*
   all pages in the memory arena consist of a header and data section
*/
typedef struct BT_page BT_page;
struct BT_page {
  BT_pageheader head;           /* ;;: TODO remove header and store all header data in BT_meta */
  union {                       /* data section */
    BT_dat      datd[BT_DAT_MAXENTRIES]; /* union view */
    BT_kv       datk[0];                 /* struct view */
    BYTE        datc[0];                 /* byte-level view */
  };
};
static_assert(sizeof(BT_page) == BT_PAGESIZE);
static_assert(BT_DAT_MAXBYTES % sizeof(BT_dat) == 0);

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

  uint32_t chk;                 /* checksum */
  pgno_t   blk_base[8];         /* block base array for striped node partition */
  uint8_t  blk_cnt;             /* currently highest valid block base */
  uint8_t  depth;               /* tree depth */
#define BP_DIRTY 0x01
#define BP_META  0x02
  uint8_t  flags;
  uint8_t  _pad1;
  pgno_t   root;
} __packed;
static_assert(sizeof(BT_meta) <= BT_DAT_MAXBYTES);

/* macro to access the metadata stored in a page's data section */
#define METADATA(p) ((BT_meta *)(void *)(p)->datc)

typedef struct BT_state BT_state;
struct BT_state {
  uint16_t      flags;          /* ;;: rem */
  int           data_fd;
  int           meta_fd; /* ;;: confident can be removed because we're not explicitly calling write() */
  char         *path;
  ULONG         branch_page_cnt; /* ;;: rem */
  ULONG         leaf_page_cnt;   /* ;;: rem */
  ULONG         depth;           /* ;;: rem */
  void         *fixaddr;
  BYTE         *map;
  BT_page      *node_freelist;
  BT_meta      *meta_pages[2];  /* double buffered */
  unsigned int  which;          /* which double-buffered db are we using? */
};



//// ===========================================================================
////                            btree internal routines

#define BT_MAXDEPTH 4           /* ;;: todo derive it */
typedef struct BT_findpath BT_findpath;
struct BT_findpath {
  BT_page *path[BT_MAXDEPTH];
  size_t idx[BT_MAXDEPTH];
  uint8_t depth;
};

/* _node_get: get a pointer to a node stored at file offset pgno */
static BT_page *
_node_get(BT_state *state, pgno_t pgno)
{
  /* TODO: eventually, once we can store more than 2M of nodes, this will need
     to reference the meta page's blk_base array to determine where a node is
     mapped. i.e:

  - receive pgno
  - find first pgno in blk_base that exceeds pgno : i
  - sector that contains node is i-1
  - appropriately offset into i-1th fixed size partition: 2M, 8M, 16M, ...



  */

  /* for now, this works because the 2M sector is at the beginning of both the
     memory arena and pma file
  */
  assert((pgno * BT_PAGESIZE) < MBYTES(2));
  return FO2PA(state->map, pgno);
}

/* ;;: I don't think we should need this if _node_alloc also returns a disc offset */
static pgno_t
_fo_get(BT_state *state, BT_page *node)
{
  uintptr_t vaddr = (uintptr_t)node;
  uintptr_t start = (uintptr_t)state->map;
  return BY2FO(vaddr - start);
}

static BT_page *
_node_alloc(BT_state *state)
{
  /* TODO: will eventually need to walk a node freelist that allocs space for
     the striped node partitions. Since this is unimplemented, just allocating
     space from first 2M */

  /* ;;: when node freelist is implemented, will we need to return the file
       offset of the node as well? This is important for splitting where we
       allocate a new node and need to store its file offset in the parent's
       data index */
  size_t width = (BYTE *)state->node_freelist - state->map;
  assert(width < MBYTES(2));
  return state->node_freelist++;
}

/* binary search a page's data section for a va. Returns a pointer to the found BT_dat */
static void *
_bt_bsearch(BT_page *page, vaof_t va)
{
  /* ;;: todo: actually bsearch rather than linear */
  for (BT_kv *kv = &page->datk[0]; kv <= BT_dat_maxva(page); kv++) {
    if (kv->va == va)
      return kv;
  }

  return 0;
}

#define is_leaf(d1, d2) ((d1) == (d2))
#define is_branch(d1, d2) (!is_leaf(d1, d2))

/* ;;: find returns a path to nodes that things should be in if they are there. */
/* a leaf has a meta page depth eq to findpath depth */
static int
_bt_find2(BT_state *state,
          BT_page *page,
          BT_findpath *path,
          uint8_t maxdepth,
          vaof_t lo,
          vaof_t hi)
{
  /* ;;: meta page stores depth (node or leaf?)
     look at root node and binsearch BT_dats where low is <= lo and high is >= hi
     If at depth of metapage (a leaf), then done
     otherwise grab node, increment depth, save node in path
  */
  if (path->depth > maxdepth)
    return ENOENT;

  size_t i = 0;
  if (is_leaf(path->depth, maxdepth)) {
    for (; i < BT_DAT_MAXKEYS - 1; i++) {
      vaof_t llo = page->datk[i].va;
      vaof_t hhi = page->datk[i + 1].va;
      if (llo <= lo && hhi >= hi) {
        path->idx[path->depth] = i;
        path->path[path->depth] = page;
        return BT_SUCC;
      }
    }
    return ENOENT;
  }
  /* then branch */
  else {
    for (; i < BT_DAT_MAXKEYS - 1; i++) {
      vaof_t llo = page->datk[i].va;
      vaof_t hhi = page->datk[i + 1].va;
      if (llo <= lo && hhi >= hi) {
        pgno_t fo = page->datk[i].fo;
        BT_page *child = _node_get(state, fo);
        path->idx[path->depth] = i;
        path->path[path->depth] = page;
        path->depth++;
        return _bt_find2(state, child, path, maxdepth, lo, hi);
      }
    }
    return ENOENT;
  }
}

static int
_bt_find(BT_state *state, BT_findpath *path, vaof_t lo, vaof_t hi)
{
  BT_meta *meta = state->meta_pages[state->which];
  BT_page *root = _node_get(state, meta->root);
  uint8_t maxdepth = meta->depth;
  return _bt_find2(state, root, path, maxdepth, lo, hi);
}

static int
_bt_findpath_is_root(BT_findpath *path)
{
  assert(path != 0);
  return path->depth == 0;
}

/* _bt_node_nexthole: find next empty space in node's data section. Returned as
   index into node->datk. If the node is full, return is BT_DAT_MAXKEYS */
static size_t
_bt_node_nexthole(BT_page *node)
{
  size_t i = 0;
  for (; i < BT_DAT_MAXKEYS; i++) {
    if (node->datk[i].fo == 0) break;
  }
  return i;
}

/* _bt_split_datcopy: copy right half of left node to right node */
static int
_bt_split_datcopy(BT_page *left, BT_page *right)
{
  size_t mid = BT_DAT_MAXKEYS / 2;
  size_t bytelen = mid * sizeof(left->datk[0]);
  /* copy rhs of left to right */
  memcpy(right->datk, &left->datk[mid], bytelen);
  /* zero rhs of left */
  ZERO(&left->datk[mid], bytelen);

  return BT_SUCC;
}


static int
_bt_dirtychild(BT_page *parent, size_t child_idx)
{
  /* ;;: given a data to the child, marks the page dirty in the parent

    ;;: is this correct? Also, TODO: generalize it */
  child_idx >>= sizeof(parent->head.dirty[0]);
  parent->head.dirty[child_idx] |= child_idx;
  /* ;;: we also have to dirty the parent of the parent and so on, right?? In
       which case, we need a path. These operations would probably be much
       simpler if child pages stored back references to parents. I believe they
       do in lmdb. */
  return BT_SUCC;
}

/* ;;: this needs to be looked at much more closely. */
static int
_bt_split(BT_state *state, BT_findpath *path, BT_page *node) /* ;;: you don't need the node argument since it's included in path. Note, it may make sense to split out the index as a param to _bt_split instead of only passing it in path to minimize funcall stack size */
{
  assert(path != 0);
  size_t depth = path->depth;

  /* if we need to split a root, need to create a new root page and increment
     btree depth */
  if (_bt_findpath_is_root(path)) {
    pgno_t pg = 0;
    BT_meta *meta = state->meta_pages[state->which];
    /* the old root is now the left child of the new root */
    BT_page *left = _node_get(state, path->idx[depth]);
    BT_page *right = _node_alloc(state);
    BT_page *new_root = _node_alloc(state);
    /* dirty metapage and set new root */
    meta->flags |= BP_DIRTY;
    meta->root = _fo_get(state, new_root);
    /* split data of left across left and right */
    _bt_split_datcopy(left, right);
    /* save left and right as first two pages of new root */
    pg = _fo_get(state, left);
    new_root->datk[0].fo = pg;
    new_root->datk[0].va = left->datk[0].va; /* ;;: not correct. va should be first va in pg */
    pg = _fo_get(state, right);
    new_root->datk[1].fo = pg;
    new_root->datk[1].va = right->datk[0].va;
    /* finally, dirty the children */
    _bt_dirtychild(new_root, 0);
    _bt_dirtychild(new_root, 1);

    return BT_SUCC;
  }

  /* non-recursive case, sufficient space in parent node */
  BT_page *parent = path->path[depth];
  BT_page *child = _node_get(state, path->idx[depth]);
  size_t hole_idx = _bt_node_nexthole(parent);

  if (hole_idx == BT_DAT_MAXKEYS) {
    /* insufficient space in parent, so split parent by recursing */
    int rc = _bt_split(state, &(BT_findpath){
        /* ;;: horrible. Just split out depth as param since the rest of the findpath data is invariant */
        .depth = path->depth -1,
        .idx = {path->idx[0], path->idx[1], path->idx[2], path->idx[3]},
        .path = {path->path[0], path->path[1], path->path[2], path->path[3]},
      },
      node);
    assert(SUCC(rc));
  }

  /* sufficient space in parent to split child and just insert */
  BT_page *new_child = _node_alloc(state);
  _bt_split_datcopy(child, new_child);
  pgno_t new_pg = _fo_get(state, new_child);
  parent->datk[hole_idx].fo = new_pg;
  parent->datk[hole_idx].va = new_child->datk[0].va;
  _bt_dirtychild(parent, hole_idx);

  return BT_SUCC;
}


static int
__bt_split(BT_state *state, BT_findpath *path, BT_page *node)
{
  /* split actually does the CoW for _bt_insert. bt_insert doesn't for non-full
     nodes.

    ;;: should we return the new node or modify path to point to it?? */
  uint8_t depth = path->depth;
  BT_page *new = _node_alloc(state);
  size_t median = BT_DAT_MAXKEYS / 2;
  memcpy(new->datk, node->datk, median);

  /* need parent node as well.
     first, dirty both old page and new page
     next, copy range (median,end) of old node to new node
       ?? Do we need to allocate two nodes?
     next, insert new node into parent, splitting parent if necessary

     ***

     split actually probably SHOULD walk the parents in path. Since we can't
     insert into a full parent. Therefore:

     null case 1:

       if root node, then allocate two new nodes A and B. Copy right half of
       old.datk to A.datk. Make B the new root and point index 0 at A and index
       1 at B.

     null case 2:

       if parent is not full, then allocate new node A. Copy right half of
       old.datk to A.datk. Right shift range of parent to make room for new
       entry. Insert ref to A next to child index i at i+1.

     recursive case:

       if parent is full, split parent.



     ;;: should we enforce that a root node is never a leaf? This may simplify
       the split logic a bit.
   */

  return 255;
}

static int
_bt_insert(BT_state *state, vaof_t lo, vaof_t hi, pgno_t fo)
{
  /* ;;: old implementation was incorrect and removed -- relied on different
       implementation of _bt_split. Need to reimplement */
  BT_findpath path = {0};
  int rc;
  if (!SUCC(rc = _bt_find(state, &path, lo, hi))) {
    return rc;
  }
  /* path now contains PATH to leaf node at path->depth handle splitting if
    necessary. Check all nodes in path if need to be split
  */
  /* "implement" basic function that bumps node allocation pointer. store
     BT_page[2M / sizeof(BT_page)] array in state. error out when size is
     exceeded.
  */
  return 255;
}

static int
_bt_delete()
{
  return 255;
}

/* bt_insert. Find interval (via _bt_find) and see if there's space in leaf to
   insert. If not, split leaf, go up path by 1, etc.

   return succ/fail
*/

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
  meta->blk_cnt = 1;
  meta->depth = 1;
  meta->flags = BP_META;
  meta->root = UINT32_MAX;      /* ;;: actually should probably be 0 */

  /* initialize the metapages */
  p1 = calloc(2, BT_PAGESIZE);
  p2 = p1 + 1;

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

  state->node_freelist = &((BT_page *)state->map)[2]; /* begin allocating nodes
                                                       on third page (first two
                                                       are for metadata) */
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

  oflags = O_RDWR | O_CREAT;
  dpath = malloc(strlen(path) + sizeof(DATANAME));
  if (!dpath) return ENOMEM;
  sprintf(dpath, "%s" DATANAME, path);

  if ((state->data_fd = open(path, oflags, mode)) == -1)
    return errno;

  if (!SUCC(rc = _bt_state_load(state)))
    goto e;

  /* ;;: this may be entirely unnecessary */
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
