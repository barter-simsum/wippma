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
#include <inttypes.h>

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
#ifdef DEBUG
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
#else
#define bp(x) ((void)(0))
#endif

/* coalescing of memory freelist currently prohibited since we haven't
   implemented coalescing of btree nodes (necessary) */
#define CAN_COALESCE 0

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

/* 4K page in bytes */
#define P2BYTES(x) ((size_t)(x) << 14)
/* the opposite of P2BYTES */
#define B2PAGES(x) ((size_t)(x) >> 14)


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


#define BT_MAPADDR  ((void *) S(0x1000,0000,0000))

/* convert addr offset to raw address */
#define OFF2ADDR(x) ((void *)((uintptr_t)(BT_MAPADDR) + (x)))
/* convert raw memory address to offset */
#define ADDR2OFF(a) ((vaof_t)((uintptr_t)(a) - (uintptr_t)BT_MAPADDR))

#define BT_PAGEBITS 14ULL
#define BT_PAGEWORD 32ULL
#define BT_PAGESIZE (1ULL << BT_PAGEBITS) /* 16K */
#define BT_ADDRSIZE (BT_PAGESIZE << BT_PAGEWORD)
#define PMA_INITIAL_PAGE_SIZE (BT_PAGESIZE * 1000)

#define BT_NOPAGE 0

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
/* #define BT_DAT_MAXKEYS 10 */
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

typedef struct BT_nlistnode BT_nlistnode;
struct BT_nlistnode {
  pgno_t pg;
  size_t sz;
  BT_nlistnode *next;
};

typedef struct BT_mlistnode BT_mlistnode;
struct BT_mlistnode {
  void *va;                     /* virtual address */
  size_t sz;                    /* size in pages */
  BT_mlistnode *next;           /* next freelist node */
};

typedef struct BT_flistnode BT_flistnode;
struct BT_flistnode {
  pgno_t pg;                    /* pgno - an offset in the persistent file */
  size_t sz;                    /* size in pages */
  BT_flistnode *next;           /* next freelist node */
};

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
  /* ;;: note, while meta_pages[which]->root stores a pgno, we may want to just
       store a pointer to root in state in addition to avoid a _node_find on it
       every time it's referenced */
  /* BT_page      *root; */
  size_t        file_len;       /* the length of the pma file */
  unsigned int  which;          /* which double-buffered db are we using? */
  BT_nlistnode *nlist;          /* node freelist */
  BT_mlistnode *mlist;          /* memory freelist */
  BT_flistnode *flist;          /* pma file freelist */
};



//// ===========================================================================
////                            btree internal routines

static void _bt_printnode(BT_page *node); /* ;;: tmp */
static int
_bt_insertdat(vaof_t lo, vaof_t hi, pgno_t fo,
              BT_page *parent, size_t childidx); /* ;;: tmp */

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
  if (pgno <= 1) return 0;      /* no nodes stored at 0 and 1 (metapages) */
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

static BT_page *                /* ;;: change to return both a file and node offset as params to function. actual return value is error code */
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
  /* ;;: todo confirm data sections are zeroed */
  /* ZERO(state->node_freelist, BT_PAGESIZE); */
  return state->node_freelist++;
}

static BT_page *
__node_alloc(BT_state *state)
{
  /* TODO implement node freelist rather than a bump pointer */
  return 0;
}

/* ;;: from our usage, _node_cow no longer needs to take indirect pointer to
     newnode. We don't ever do anything with it */
static int
_node_cow(BT_state *state, BT_page *node, BT_page **newnode, pgno_t *pgno)
{
  BT_page *ret = _node_alloc(state);
  memcpy(ret->datk, node->datk, sizeof node->datk[0] * BT_DAT_MAXENTRIES);
  *pgno = _fo_get(state, ret);
  *newnode = ret;
  return BT_SUCC;
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

static size_t
_bt_childidx(BT_page *node, vaof_t lo, vaof_t hi)
/* looks up the child index in a parent node. If not found, return is
   BT_DAT_MAXKEYS */
{
  if (!node) bp(0);             /* ;;: tmp. debugging sigsegv */
  size_t i = 0;
  for (; i < BT_DAT_MAXKEYS - 1; i++) {
    vaof_t llo = node->datk[i].va;
    vaof_t hhi = node->datk[i+1].va;
    if (llo <= lo && hhi >= hi)
      return i;
  }
  return BT_DAT_MAXKEYS;
}

/* ;;: find returns a path to nodes that things should be in if they are there. */
/* a leaf has a meta page depth eq to findpath depth */
static int
_bt_find2(BT_state *state,
          BT_page *node,
          BT_findpath *path,
          uint8_t maxdepth,
          vaof_t lo,
          vaof_t hi)
{
  /* ;;: meta node stores depth (node or leaf?)
     look at root node and binsearch BT_dats where low is <= lo and high is >= hi
     If at depth of metapage (a leaf), then done
     otherwise grab node, increment depth, save node in path
  */
  if (path->depth > maxdepth)
    return ENOENT;

  if (node == 0) bp(0);
  assert(node != 0);

  size_t i;
  if ((i = _bt_childidx(node, lo, hi)) == BT_DAT_MAXKEYS)
    return ENOENT;

  if (is_leaf(path->depth, maxdepth)) {
    path->idx[path->depth] = i;
    path->path[path->depth] = node;
    return BT_SUCC;
  }
  /* then branch */
  else {
    pgno_t fo = node->datk[i].fo;
    BT_page *child = _node_get(state, fo);
    path->idx[path->depth] = i;
    path->path[path->depth] = node;
    path->depth++;
    return _bt_find2(state, child, path, maxdepth, lo, hi);
  }
}

static void
_bt_root_new(BT_page *root)
{
  root->datk[0].va = 0;
  root->datk[0].fo = 0;
  root->datk[1].va = UINT32_MAX;
  root->datk[1].fo = 0;
}

static int
_bt_find(BT_state *state, BT_findpath *path, vaof_t lo, vaof_t hi)
{
  path->depth = 1;
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

/* _bt_numkeys: find next empty space in node's data section. Returned as
   index into node->datk. If the node is full, return is BT_DAT_MAXKEYS */
static size_t
_bt_numkeys(BT_page *node)
{
  size_t i = 1;
  for (; i < BT_DAT_MAXKEYS; i++) {
    if (node->datk[i].va == 0) break;
  }
  return i;
}

static int
_bt_datshift(BT_page *node, size_t i, size_t n)
/* shift data segment at i over by n KVs */
{
  assert(i+n < BT_DAT_MAXKEYS); /* check buffer overflow */
  size_t siz = sizeof node->datk[0];
  size_t bytelen = (BT_DAT_MAXKEYS - i) * siz;
  memmove(&node->datk[i+n], &node->datk[i], bytelen);
  ZERO(&node->datk[i], n * siz);
  return BT_SUCC;
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
  ZERO(&left->datk[mid], bytelen); /* ;;: note, this would be unnecessary if we stored node.N */
  left->datk[mid].va = right->datk[0].va;

  return BT_SUCC;
}

static int
_bt_ischilddirty(BT_page *parent, size_t child_idx)
{
  assert(child_idx < 2048);
  uint8_t flag = parent->head.dirty[child_idx >> 3];
  return flag & (1 << (child_idx & 0x7));
}

/* ;;: todo: name the 0x8 and 4 literals and/or generalize */
static int
_bt_dirtychild(BT_page *parent, size_t child_idx)
{
  assert(child_idx < 2048);
  /* although there's nothing theoretically wrong with dirtying a dirty node,
     there's probably a bug if we do it since a we only dirty a node when it's
     alloced after a split or CoWed */
  assert(!_bt_ischilddirty(parent, child_idx));
  uint8_t *flag = &parent->head.dirty[child_idx >> 3];
  *flag |= 1 << (child_idx & 0x7);
  return BT_SUCC;
}

#define IS_ROOT(path) ((path)->depth == 0)

/* ;:: assert that the node is dirty when splitting */
static int
_bt_split_child(BT_state *state, BT_page *node, size_t i, pgno_t *newchild)
{
  /* ;;: todo: better error handling */
  int rc = BT_SUCC;
  BT_page *left = _node_get(state, node->datk[i].fo);
  size_t N = _bt_numkeys(left);
  vaof_t lo = left->datk[0].va;
  vaof_t hi = left->datk[N-1].va;
  BT_page *right = _node_alloc(state);
  if (right == 0)
    return ENOMEM;
  if (!SUCC(rc = _bt_split_datcopy(left, right)))
    return rc;

  /* ;;: todo, we can ofc unconditionally dirty the right child since it was
       freshly alloced. However, the left child isn't guaranteed
       dirty. Therefore:
         - if left is NOT dirty, CoW it and dirty it. Call data copy on
         CoWed left and freshly alloced right

         - if it IS ALREADY dirty, then just call data copy on original left and
           freshly alloced right

     alternatively, assert that left is already dirt. Responsibility falls on
     caller (insert) to ensure node being inserted into is either dirty or CoWed
  */

  /* dirty right child */
  _bt_dirtychild(node, i+1);

  /* insert reference to right child into parent node */
  bp(0);
  _bt_insertdat(lo, hi, *newchild, node, i+1);

  /* ;;: fix this */
  *newchild = _fo_get(state, right);
  state->meta_pages[state->which]->depth += 1;

  return BT_SUCC;
}

/* ;;: since we won't be rebalancing on delete, but rather on insert, you should add rebalance logic to _bt_insert2 which checks the degree of a node and rebalances if less than minimum */

static int
_bt_rebalance(BT_state *state, BT_page *node)
{
  return 255;
}

/* insert lo, hi, and fo in parent's data section for childidx */
static int
_bt_insertdat(vaof_t lo, vaof_t hi, pgno_t fo,
              BT_page *parent, size_t childidx)
{
  DPRINTF("BEFORE INSERT lo %" PRIu32 " hi %" PRIu32 " fo %" PRIu32, lo, hi, fo);
  /* _bt_printnode(parent); */

  /* ;;: TODO confirm this logic is appropriate for branch nodes. (It /should/
       be correct for leaf nodes) */
  vaof_t llo = parent->datk[childidx].va;
  vaof_t hhi = parent->datk[childidx+1].va;

  /* duplicate */
  if (llo == lo && hhi == hi) {
    parent->datk[childidx].fo = fo;
    return BT_SUCC;
  }

  if (llo == lo) {
    _bt_datshift(parent, childidx + 1, 1);
    vaof_t oldfo = parent->datk[childidx].fo;
    parent->datk[childidx].fo = fo;
    parent->datk[childidx+1].va = hi;
    parent->datk[childidx+1].fo = oldfo + (hi - llo);
  }
  else if (hhi == hi) {
    _bt_datshift(parent, childidx + 1, 1);
    parent->datk[childidx+1].va = lo;
    parent->datk[childidx+1].fo = fo;
  }
  else {
    _bt_datshift(parent, childidx + 1, 2);
    parent->datk[childidx+1].va = lo;
    parent->datk[childidx+1].fo = fo;
    parent->datk[childidx+2].va = hi;
    pgno_t lfo = parent->datk[childidx].fo;
    vaof_t lva = parent->datk[childidx].va;
    parent->datk[childidx+2].fo = (lfo == 0)
      ? 0
      : lfo + (hi - lva);
  }

  DPUTS("AFTER INSERT");
  /* _bt_printnode(parent); */
  return BT_SUCC;
}

/* ;;: todo, update meta->depth when we add a row. Should this be done in
     _bt_rebalance? */
static int
_bt_insert2(BT_state *state, vaof_t lo, vaof_t hi, pgno_t fo,
        BT_page *node, size_t depth)
{
  /* ;;: to be written in such a way that node is guaranteed both dirty and
       non-full */

  /* ;;: remember:
     - You need to CoW+dirty a node when you insert a non-dirty node.
     - You need to insert into a node when:
       - It's a leaf
       - It's a branch and you CoWed the child
     - Hence, all nodes in a path to a leaf being inserted into need to already
     be dirty or explicitly CoWed. Splitting doesn't actually factor into this
     decision afaict.
  */

  assert(node);

  int rc = 255;
  size_t N = 0;
  size_t childidx = _bt_childidx(node, lo, hi);
  assert(childidx != BT_DAT_MAXKEYS);
  BT_meta *meta = state->meta_pages[state->which];

  if (depth < meta->depth) {
    pgno_t childpgno = node->datk[childidx].fo;
    BT_page *child = _node_get(state, childpgno);
    N = _bt_numkeys(child);
  }

  /* nullcond: node is a leaf */
  if (meta->depth == depth) {
    /* guaranteed non-full and dirty by n-1 recursive call, so just insert */
    return _bt_insertdat(lo, hi, fo, node, childidx);
  }

  /* do we need to CoW the child node? */
  if (!_bt_ischilddirty(node, childidx)) {
    BT_page *newchild;
    pgno_t pgno;
    _node_cow(state, node, &newchild, &pgno);
    node->datk[childidx].fo = pgno;
    _bt_dirtychild(node, childidx);
  }

  /* do we need to split the child node? */
  if (N >= BT_DAT_MAXKEYS - 2) {
      pgno_t rchild_pgno;
      if (!SUCC(rc = _bt_split_child(state, node, childidx, &rchild_pgno)))
        return rc;

      
      /* ;;: FIX FIX FIX FIX FIX { */

      /* since we split the child's data, recalculate the child idx */
      /* ;;: note, this can be simplified into a conditional i++ */
      childidx = _bt_childidx(node, lo, hi);
      BT_page *rchild = _node_get(state, rchild_pgno);
      /* _bt_insertdat(lo, hi, rchild_pgno, node, childidx); */


      /* now insert the new child into parent... */
      /* ;;: may need to be fixed */
      /* _bt_datshift(node, childidx+1, 1); */
      /* BT_page *lchild = _node_get(state, node->datk[childidx].fo); */
      /* BT_page *rchild = _node_get(state, rchild_pgno); */
      /* node->datk[childidx+1].va = rchild->datk[0].va; */
      /* node->datk[childidx+1].fo = rchild_pgno; */

      /* since we split the child's data, recalculate the child idx */
      /* ;;: note, this can be simplified into a conditional i++ */
      childidx = _bt_childidx(node, lo, hi);

      /* } */
  }

  /* the child is now guaranteed non-full (split) and dirty. Recurse */
  BT_page *child = _node_get(state, node->datk[childidx].fo);
  return _bt_insert2(state, lo, hi, fo, child, depth+1);
}

static int
_bt_insert(BT_state *state, vaof_t lo, vaof_t hi, pgno_t fo)
/* handles CoWing/splitting of the root page since it's special cased. Then
   passes the child matching hi/lo to _bt_insert2 */
{
  int rc;

  BT_meta *meta = state->meta_pages[state->which];
  BT_page *root = _node_get(state, meta->root);

  size_t N = _bt_numkeys(root);

  /* if the root isn't dirty, cow it and flip the which */
  if (!(meta->flags & BP_DIRTY)) {
    BT_page *newroot;
    pgno_t  newrootpg;

    if (!SUCC(rc = _node_cow(state, root, &newroot, &newrootpg)))
      return rc;

    /* switch to other metapage and dirty it */
    state->which = state->which ? 0 : 1;
    meta = state->meta_pages[state->which];
    meta->flags |= BP_DIRTY;
    meta->root = newrootpg;
    root = newroot;
  }

  /* CoW root's child if it isn't already dirty */
  size_t childidx = _bt_childidx(root, lo, hi);
  if (meta->depth > 1
      && !_bt_ischilddirty(root, childidx)) {
    BT_page *child = _node_get(state, root->datk[childidx].fo);
    BT_page *newchild;
    pgno_t  newchildpg;
    _node_cow(state, child, &newchild, &newchildpg);
    root->datk[childidx].fo = newchildpg;
    _bt_dirtychild(root, childidx);
  }

  /* before calling into recursive insert, handle root splitting since it's
     special cased (2 allocs) */
  if (N >= BT_DAT_MAXKEYS - 2) { /* ;;: remind, fix all these conditions to be - 2 */
    pgno_t pg = 0;

    /* the old root is now the left child of the new root */
    BT_page *left = root;
    BT_page *right = _node_alloc(state);
    BT_page *rootnew = _node_alloc(state);

    /* split root's data across left and right nodes */
    _bt_split_datcopy(left, right);
    /* save left and right in new root's .data */
    pg = _fo_get(state, left);
    rootnew->datk[0].fo = pg;
    rootnew->datk[0].va = 0;
    pg = _fo_get(state, right);
    rootnew->datk[1].fo = pg;
    rootnew->datk[1].va = right->datk[0].va;
    rootnew->datk[2].va = UINT32_MAX;
    /* dirty new root's children */
    _bt_dirtychild(rootnew, 0);
    _bt_dirtychild(rootnew, 1);
    /* update meta page information. (root and depth) */
    pg = _fo_get(state, rootnew);
    meta->root = pg;
    meta->depth += 1;
    root = rootnew;
  }

  /*
    meta is dirty
    root is dirty and split if necessary
    root's child in insert path is dirty and split if necessary
    finally, recurse on child
  */
  return _bt_insert2(state, lo, hi, fo, root, 1);
  /* return _bt_insert2(state, lo, hi, fo, child, 1); */
}

/* ;;: wip */
/* ;;: inspired by lmdb's MDB_pageparent. While seemingly unnecessary for
     _bt_insert, this may be useful for _bt_delete */
typedef struct BT_ppage BT_ppage;
struct BT_ppage {
  BT_page *node;
  BT_page *parent;
};

/* static int */
/* _bt_ppage_get(BT_state *state, size_t childidx, BT_ppage **bpp) */
/* { */
/*   BT_page *child = _node_get(state, pgno); */
/*   (*bpp)->node = child; */
/* } */

static int
_bt_delete(BT_state *state, vaof_t lo, vaof_t hi)
{
  /* ;;: tmp, implement coalescing of zero ranges and merging/rebalancing of
       nodes */
  return _bt_insert(state, lo, hi, 0);
}

static BT_mlistnode *
_mlist_create2(BT_state *state, BT_page *node, uint8_t maxdepth, uint8_t depth)
{
  /* leaf */
  if (depth == maxdepth) {
    BT_mlistnode *head, *prev;
    head = prev = calloc(1, sizeof *head);

    size_t i = 0;
    BT_kv *kv = &node->datk[i];
    while (i < BT_DAT_MAXKEYS - 1) {
#if CAN_COALESCE
      /* free and contiguous with previous mlist node: merge */
      if (kv->fo == 0
          && ADDR2OFF(prev->va) + P2BYTES(prev->sz) == kv->va) {
        vaof_t hi = node->datk[i+1].va;
        vaof_t lo = kv->va;
        size_t len = B2PAGES(hi - lo);
        prev->sz += len;
      }
      /* free but not contiguous with previous mlist node: append new node */
      else if (kv->fo == 0) {
#endif
        BT_mlistnode *new = calloc(1, sizeof *new);
        vaof_t hi = node->datk[i+1].va;
        vaof_t lo = kv->va;
        size_t len = B2PAGES(hi - lo);
        new->sz = len;
        new->va = OFF2ADDR(lo);
        prev->next = new;
        prev = new;
#if CAN_COALESCE
      }
#endif

      kv = &node->datk[++i];
    }
    return head;
  }

  /* branch */
  size_t i = 0;
  BT_mlistnode *head, *prev;
  head = prev = 0;
  for (; i < BT_DAT_MAXKEYS; ++i) {
    BT_kv kv = node->datk[i];
    if (kv.fo == BT_NOPAGE)
      continue;
    BT_page *child = _node_get(state, kv.fo);
    BT_mlistnode *new = _mlist_create2(state, child, maxdepth, depth+1);
    if (head == 0) {
      head = prev = new;
    }
    else {
      /* just blindly append and unify the ends afterward */
      prev->next = new;
    }
  }
  return 0;
}

static int
_mlist_create(BT_state *state)
{
  BT_meta *meta = state->meta_pages[state->which];
  BT_page *root = _node_get(state, meta->root);
  uint8_t maxdepth = meta->depth;
  BT_mlistnode *head = _mlist_create2(state, root, maxdepth, 0);

  /*
    trace the full freelist and unify nodes one last time
    NB: linking the leaf nodes would make this unnecessary
  */
#if CAN_COALESCE
  BT_mlistnode *p = head;
  BT_mlistnode *n = head->next;
  while (n) {
    size_t llen = P2BYTES(p->sz);
    uintptr_t laddr = (uintptr_t)p->va;
    uintptr_t raddr = (uintptr_t)n->va;
    /* contiguous: unify */
    if (laddr + llen == raddr) {
      p->sz += n->sz;
      p->next = n->next;
      free(n);
    }
  }
#endif

  state->mlist = head;
  return BT_SUCC;
}

static int
_mlist_delete(BT_state *state)
{
  BT_mlistnode *head, *prev;
  head = prev = state->mlist;
  while (head->next) {
    prev = head;
    head = head->next;
    free(prev);
  }
  state->mlist = 0;
  return BT_SUCC;
}

static void
_flist_split(BT_flistnode *head, BT_flistnode **left, BT_flistnode **right)
/* split flist starting at head into two lists, left and right at the midpoint
   of head */
{
  assert(head != 0);
  BT_flistnode *slow, *fast;
  slow = head; fast = head->next;

  while (fast) {
    fast = fast->next;
    if (fast) {
      slow = slow->next;
      fast = fast->next;
    }
  }

  *left = head;
  *right = slow->next;
  slow->next = 0;
}

static BT_flistnode *
_flist_merge2(BT_flistnode *l, BT_flistnode *r)
/* returns the furthest node in l that has a pg less than the first node in r */
{
  assert(l);
  assert(r);

  BT_flistnode *curr, *prev;
  prev = l;
  curr = l->next;

  while (curr) {
    if (curr->pg < r->pg) {
      prev = curr;
      curr = curr->next;
    }
  }

  if (prev->pg < r->pg)
    return prev;

  return 0;
}

static BT_flistnode *
_flist_merge(BT_flistnode *l, BT_flistnode *r)
/* merge two sorted flists, l and r and return the sorted result */
{
  BT_flistnode *head;

  if (!l) return r;
  if (!r) return l;

  while (l && r) {
    if (l->next == 0) {
      l->next = r;
      break;
    }
    if (r->next == 0) {
      break;
    }

    BT_flistnode *ll = _flist_merge2(l, r);
    BT_flistnode *rnext = r->next;
    /* insert head of r into appropriate spot in l */
    r->next = ll->next;
    ll->next = r;
    /* adjust l and r heads */
    l = ll->next;
    r = rnext;
  }

  return head;
}

BT_flistnode *
_flist_mergesort(BT_flistnode *head)
{
  if (head == 0 || head->next == 0)
    return head;

  BT_flistnode *l, *r;
  _flist_split(head, &l, &r);

  /* ;;: todo, make it non-recursive. Though, shouldn't matter as much here
       since O(log n). merge already non-recursive */
  _flist_mergesort(l);
  _flist_mergesort(r);

  return _flist_merge(l, r);
}

BT_flistnode *
_flist_create2(BT_state *state, BT_page *node, uint8_t maxdepth, uint8_t depth)
{
  /* leaf */
  if (depth == maxdepth) {
    BT_flistnode *head, *prev;
    head = prev = calloc(1, sizeof(*head));

    /* ;;: fixme the head won't get populated in this logic */
    size_t i = 0;
    BT_kv *kv = &node->datk[i];
    while (i < BT_DAT_MAXKEYS - 1) {
      /* Just blindly append nodes since they aren't guaranteed sorted */
      BT_flistnode *new = calloc(1, sizeof *new);
      vaof_t hi = node->datk[i+1].va;
      vaof_t lo = kv->va;
      size_t len = B2PAGES(hi - lo);
      pgno_t fo = kv->fo;
      new->sz = len;
      new->pg = fo;
      prev->next = new;
      prev = new;

      kv = &node->datk[++i];
    }
    return head;
  }

  /* branch */
  size_t i = 0;
  BT_flistnode *head, *prev;
  head = prev = 0;
  for (; i < BT_DAT_MAXKEYS; ++i) {
    BT_kv kv = node->datk[i];
    if (kv.fo == BT_NOPAGE)
      continue;
    BT_page *child = _node_get(state, kv.fo);
    BT_flistnode *new = _flist_create2(state, child, maxdepth, depth+1);
    if (head == 0) {
      head = prev = new;
    }
    else {
      /* just blindly append and unify the ends afterward */
      prev->next = new;
    }
  }
  return 0;
}

static int
_flist_create(BT_state *state)
{
  BT_meta *meta = state->meta_pages[state->which];
  BT_page *root = _node_get(state, meta->root);
  uint8_t maxdepth = meta->depth;
  BT_flistnode *head = _flist_create2(state, root, maxdepth, 0);

  if (head == 0)
    return BT_SUCC;

  /* sort the freelist */
  _flist_mergesort(head);

  /* merge contiguous regions after sorting */
  BT_flistnode *p = head;
  BT_flistnode *n = head->next;
  while (n) {
    size_t llen = p->sz;
    pgno_t lfo = p->pg;
    pgno_t rfo = n->pg;
    /* contiguous: unify */
    if (lfo + llen == rfo) {
      p->sz += n->sz;
      p->next = n->next;
      free(n);
    }
  }

  state->flist = head;
  return BT_SUCC;
}

static int
_flist_delete(BT_state *state)
{
  BT_flistnode *head, *prev;
  head = prev = state->flist;
  while (head->next) {
    prev = head;
    head = head->next;
    free(prev);
  }
  state->flist = 0;
  return BT_SUCC;
}

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
_bt_state_meta_new(BT_state *state)
#define INITIAL_ROOTPG 2
{
  BT_page *p1, *p2, *root;
  BT_meta meta = {0};
  int rc, pagesize;

  TRACE();

  root = &((BT_page *)state->map)[INITIAL_ROOTPG];
  _bt_root_new(root);

  pagesize = sizeof *p1;

  /* initialize meta struct */
  meta.magic = BT_MAGIC;
  meta.version = BT_VERSION;
  meta.last_pg = 1;
  meta.txnid = 0;
  meta.fix_addr = BT_MAPADDR;
  meta.blk_cnt = 1;
  meta.depth = 1;
  meta.flags = BP_META;
  /* meta.root = UINT32_MAX;      /\* ;;: actually should probably be 0 *\/ */
  meta.root = INITIAL_ROOTPG;

  /* initialize the metapages */
  p1 = &((BT_page *)state->map)[0];
  p2 = &((BT_page *)state->map)[1];
  /* p1 = calloc(2, BT_PAGESIZE); */
  /* p2 = p1 + 1; */

  /* copy the metadata into the metapages */
  memcpy(METADATA(p1), &meta, sizeof meta);
  /* ;;: todo, should the second metapage actually share a .root with the
       first?? */
  memcpy(METADATA(p2), &meta, sizeof meta);

  /* sync new meta pages with disk */
  /* rc = write(state->data_fd, p1, pagesize * 2); */

  /* free(p1); */
  /* if (rc == pagesize * 2) */
  /*   return BT_SUCC; */
  /* else */
  /*   return errno; */
  return BT_SUCC;
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
    state->file_len = PMA_INITIAL_PAGE_SIZE;
    new = 1;
  }

  state->map = mmap(BT_MAPADDR,
                    BT_ADDRSIZE,
                    PROT_READ | PROT_WRITE,
                    MAP_FIXED | MAP_SHARED,
                    state->data_fd,
                    0);

  state->node_freelist = &((BT_page *)state->map)[3]; /* begin allocating nodes
                                                       on third page (first two
                                                       are for metadata) */
  /* new db, so populate metadata */
  if (new) {
    /* ;;: tmp, implement differently. Set initial size of pma file */
    lseek(state->data_fd, state->file_len, SEEK_SET);
    write(state->data_fd, "", 1);

    if (!SUCC(rc = _bt_state_meta_new(state))) {
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

/* ;;: TODO, when persistence has been implemented, _bt_falloc will probably
     need to handle extension of the file with appropriate striping. i.e. if no
     space is found on the freelist, save the last entry, expand the file size,
     and set last_entry->next to a new node representing the newly added file
     space */
static pgno_t
_bt_falloc(BT_state *state, size_t pages)
{
  /* walk the persistent file freelist and return a pgno with sufficient
     contiguous space for pages */
  BT_flistnode **n = &state->flist;
  pgno_t ret = 0;

  /* first fit */
  /* ;;: is there any reason to use a different allocation strategy for disk? */
  for (; *n; n = &(*n)->next) {
    /* perfect fit */
    if ((*n)->sz == pages) {
      pgno_t ret;
      ret = (*n)->pg;
      *n = (*n)->next;
      return ret;
    }
    /* larger than necessary: shrink the node */
    if ((*n)->sz > pages) {
      pgno_t ret;
      ret = (*n)->pg;
      (*n)->sz -= pages;
      (*n)->pg = (*n)->pg + pages;
      return ret;
    }
  }

  bp(0);                        /* ;;: tmp dbg. no space found */
  return 0;
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

  if (mkdir(path, 0774) == -1)
    return errno;

  if ((state->data_fd = open(dpath, oflags, mode)) == -1)
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

int
bt_state_close(BT_state *state)
{
  int rc;
  if (state->data_fd != -1) CLOSE_FD(state->data_fd);
  if (state->meta_fd != -1) CLOSE_FD(state->meta_fd);

  /* ;;: wip delete the file because we haven't implemented persistence yet */
  if (!SUCC(rc = remove(state->path)))
    return rc;

  _mlist_delete(state);
  _flist_delete(state);

  return BT_SUCC;
}

void *
bt_malloc(BT_state *state, size_t pages)
{
  BT_mlistnode **n = &state->mlist;
  void *ret = 0;
  /* first fit */
  for (; *n; n = &(*n)->next) {
    /* perfect fit */
    if ((*n)->sz == pages) {
      ret = (*n)->va;
      *n = (*n)->next;
      break;
    }
    /* larger than necessary: shrink the node */
    if ((*n)->sz > pages) {
      ret = (*n)->va;
      (*n)->sz -= pages;
      (*n)->va = (BT_page *)(*n)->va + pages;
      break;
    }
  }

  pgno_t pgno = _bt_falloc(state, pages);
  bp(pgno != 0);
  _bt_insert(state,
             ADDR2OFF(ret),
             ADDR2OFF(ret) + P2BYTES(pages),
             pgno);

  bp(ret != 0);
  return ret;
}


//// ===========================================================================
////                                    tests

/* ;;: obv this should be moved to a separate file */
static void
_sham_sync_clean(BT_page *node)
{
  for (uint8_t *dit = &node->head.dirty[0]
         ; dit < &node->head.dirty[sizeof(node->head.dirty) - 1]
         ; dit++) {
    *dit = 0;
  }
}

static void
_sham_sync2(BT_state *state, BT_page *node, uint8_t depth, uint8_t maxdepth)
{
  if (depth == maxdepth) return;

  /* clean node */
  _sham_sync_clean(node);

  /* then recurse and clean all children with DFS */
  size_t N = _bt_numkeys(node);
  for (size_t i = 1; i < N; ++i) {
    BT_kv kv = node->datk[i];
    pgno_t childpg = kv.fo;
    BT_page *child = _node_get(state, childpg);
    _sham_sync2(state, child, depth++, maxdepth);
  }
}

static void
_sham_sync(BT_state *state)
{
  /* walk the tree and unset the dirty bit from all pages */
  BT_meta *meta = state->meta_pages[state->which];
  BT_page *root = _node_get(state, meta->root);
  meta->flags ^= BP_DIRTY;      /* unset the meta dirty flag */
  _sham_sync2(state, root, 0, meta->depth);
}

static void
_bt_printnode(BT_page *node)
{
  printf("node: %p\n", node);
  printf("data: \n");
  for (size_t i = 0; i < BT_DAT_MAXKEYS; ++i) {
    if (i && node->datk[i].va == 0)
      break;
    printf("[%5zu] %10x %10x\n", i, node->datk[i].va, node->datk[i].fo);
  }
}

static void
_test_nodeinteg(BT_state *state, BT_findpath *path,
                vaof_t lo, vaof_t hi, pgno_t pg)
{
  size_t childidx = 0;
  BT_page *parent = 0;

  assert(SUCC(_bt_find(state, path, lo, hi)));
  parent = path->path[path->depth];
  /* _bt_printnode(parent); */
  childidx = path->idx[path->depth];
  assert(parent->datk[childidx].fo == pg);
  assert(parent->datk[childidx].va == lo);
  assert(parent->datk[childidx+1].va == hi);
}

int main(int argc, char *argv[])
{
  BT_state *state;
  BT_findpath path = {0};

  bt_state_new(&state);
  bt_state_open(state, "./pmatest", 0, 0644);
  _mlist_create(state);
  _flist_create(state);

  /* ;;: haven't implemented bt_state_close yet - though it shouldn't be
     terribly hard, so should just run these tests independently by commenting
     them out for now */

  
  //// ===========================================================================
  ////                                    test1

  /* splitting tests. Insert sufficient data to force splitting. breakpoint before
     that split is performed */

  /* the hhi == hi case for more predictable splitting math */
  vaof_t lo = 10;
  vaof_t hi = BT_DAT_MAXKEYS * 4;
  pgno_t pg = 1;                /* dummy value */
  for (size_t i = 0; i < BT_DAT_MAXKEYS * 4; ++i) {
    if (i % (BT_DAT_MAXKEYS - 2) == 0)
      bp(0);                    /* breakpoint on split case */
    _bt_insert(state, lo, hi, pg);
    _test_nodeinteg(state, &path, lo++, hi, pg++);
  }

  int which = state->which;
  /* sham sync and re-run insertions */
  _sham_sync(state);
  for (size_t i = 0; i < BT_DAT_MAXKEYS * 4; ++i) {
    _bt_insert(state, lo, hi, pg);
    _test_nodeinteg(state, &path, lo++, hi, pg++);
  }
  assert(which != state->which);

  bt_state_close(state);


  
  //// ===========================================================================
  ////                                    test2

  bt_state_open(state, "./pmatest", 0, 644);
  _mlist_create(state);
  _flist_create(state);

  /* varieties of insert */

  /* 2.1 exact match */
  lo = 0x10;
  hi = 0x20;
  pg = 0xFFFFFFFF;

  bp(0);
  _bt_insert(state, lo, hi, pg);
  _bt_insert(state, lo, hi, pg);

  /* ;;: you should also probably assert the data is laid out in datk at you expect */
  _test_nodeinteg(state, &path, lo, hi, pg);

  _bt_delete(state, lo, hi);

  /* 2.2 neither bounds match */
  bp(0);
  _bt_insert(state, lo, hi, pg);
  _bt_insert(state, lo+2, hi-2, pg-1);

  _test_nodeinteg(state, &path, lo, hi, pg);
  _test_nodeinteg(state, &path, lo+2, hi-2, pg-1);

  _bt_delete(state, lo, hi);
  _bt_delete(state, lo+2, hi-2);

  /* 2.3 space to right */
  bp(0);
  _bt_insert(state, lo, hi, pg);
  _bt_insert(state, lo, hi-2, pg-1);

  _test_nodeinteg(state, &path, lo, hi, pg);
  _test_nodeinteg(state, &path, lo, hi-2, pg-1);

  _bt_delete(state, lo, hi);
  _bt_delete(state, lo, hi-2);

  /* 2.4 space to left */
  bp(0);

  _bt_insert(state, lo, hi, pg);
  _bt_insert(state, lo+2, hi, pg-1);

  _test_nodeinteg(state, &path, lo, hi, pg);
  _test_nodeinteg(state, &path, lo+2, hi, pg-1);

  _bt_delete(state, lo, hi);
  _bt_delete(state, lo+2, hi);

  bt_state_close(state);

  return 0;
}
