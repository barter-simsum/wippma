#pragma once


//// ===========================================================================
////                                  wip wip wip 

/* ;;: just copied straight over from lmdb. We don't need all of these routines
     nor do we need to export btree operations. Just a reminder while writing
     the btree functionality
*/
#include <stdlib.h>
#include <stdint.h>

struct btree		*btree_open_fd(int fd, unsigned int flags);

struct btree		*btree_open(const char *path, unsigned int flags,
			    mode_t mode);
void			 btree_close(struct btree *bt);
const struct btree_stat	*btree_stat(struct btree *bt);

struct btree_txn	*btree_txn_begin(struct btree *bt, int rdonly);
int			 btree_txn_commit(struct btree_txn *txn);
void			 btree_txn_abort(struct btree_txn *txn);

int			 btree_txn_get(struct btree *bt, struct btree_txn *txn,
			    struct btval *key, struct btval *data);
int			 btree_txn_put(struct btree *bt, struct btree_txn *txn,
			    struct btval *key, struct btval *data,
			    unsigned int flags);
int			 btree_txn_del(struct btree *bt, struct btree_txn *txn,
			    struct btval *key, struct btval *data);

#define btree_get(bt, key, data)	 \
			 btree_txn_get(bt, NULL, key, data)
#define btree_put(bt, key, data, flags)	 \
			 btree_txn_put(bt, NULL, key, data, flags)
#define btree_del(bt, key, data)	 \
			 btree_txn_del(bt, NULL, key, data)

void			 btree_set_cache_size(struct btree *bt,
			    unsigned int cache_size);
unsigned int		 btree_get_flags(struct btree *bt);
const char		*btree_get_path(struct btree *bt);

#define btree_cursor_open(bt)	 \
			 btree_txn_cursor_open(bt, NULL)
struct cursor		*btree_txn_cursor_open(struct btree *bt,
			    struct btree_txn *txn);
void			 btree_cursor_close(struct cursor *cursor);
int			 btree_cursor_get(struct cursor *cursor,
			    struct btval *key, struct btval *data,
			    enum cursor_op op);

int			 btree_sync(struct btree *bt);
int			 btree_compact(struct btree *bt);
int			 btree_revert(struct btree *bt);

int			 btree_cmp(struct btree *bt, const struct btval *a,
			     const struct btval *b);
void			 btval_reset(struct btval *btv);
