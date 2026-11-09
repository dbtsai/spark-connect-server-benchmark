/* Exercise the real C release callbacks without starting a JVM. */
#include "../native/bridge.c"
#include <assert.h>
#include <pthread.h>

/* The C bridge links to these Rust entry points; this test does not invoke them. */
int64_t ferrum_start(const uint8_t *p, size_t n, void *b, BridgeBuffer *e) {
  (void) p; (void) n; (void) b; (void) e; return 0;
}
int64_t ferrum_engine_start(const uint8_t *p, size_t n, void *b, BridgeBuffer *e) {
  (void) p; (void) n; (void) b; (void) e; return 0;
}
int ferrum_address(int64_t id, BridgeBuffer *o, BridgeBuffer *e) {
  (void) id; (void) o; (void) e; return -1;
}
int ferrum_stop(int64_t id, BridgeBuffer *e) { (void) id; (void) e; return -1; }

static atomic_int finalizations;
static void *released_owner;

static void defer(void *lease, void *owner) {
  assert(lease == &finalizations);
  released_owner = owner;
  atomic_fetch_add(&finalizations, 1);
}

static ArrowExportOwner *owner(unsigned refs) {
  ArrowExportOwner *result = calloc(1, sizeof(*result));
  assert(result != NULL);
  atomic_init(&result->references, refs);
  result->lease = &finalizations;
  result->defer_release = defer;
  return result;
}

static struct ArrowArray node(ArrowExportOwner *shared, int64_t *values) {
  struct ArrowArray result = {0};
  result.length = 3;
  result.n_buffers = 2;
  result.buffers = calloc(2, sizeof(*result.buffers));
  assert(result.buffers != NULL);
  result.buffers[1] = values;
  ArrowNodePrivate *private = calloc(1, sizeof(*private));
  assert(private != NULL);
  private->owner = shared;
  private->capacities = calloc(2, sizeof(*private->capacities));
  assert(private->capacities != NULL);
  private->capacities[1] = 3 * sizeof(*values);
  result.private_data = private;
  result.release = arrow_node_release;
  return result;
}

static void *release_on_worker(void *pointer) {
  struct ArrowArray *array = pointer;
  array->release(array);
  return NULL;
}

int main(void) {
  int64_t data[] = {7, 11, 13};
  ArrowExportOwner *shared = owner(2);
  struct ArrowArray parent = node(shared, data);
  parent.n_children = 1;
  parent.children = calloc(1, sizeof(*parent.children));
  assert(parent.children != NULL);
  parent.children[0] = malloc(sizeof(*parent.children[0]));
  assert(parent.children[0] != NULL);
  *parent.children[0] = node(shared, data);
  assert(eng_arrow_buffer_size(&parent, 1) == (int64_t) sizeof(data));
  assert(eng_arrow_buffer_size(&parent, -1) == -1);
  assert(parent.buffers[1] == data);

  /* A C Data consumer moves a descriptor by clearing its original release slot. */
  struct ArrowArray moved = *parent.children[0];
  parent.children[0]->release = NULL;
  parent.release(&parent);
  assert(atomic_load(&finalizations) == 0);
  assert(eng_arrow_buffer_size(&parent, 1) == -1);
  assert(moved.buffers[1] == data);
  assert(data[0] == 7 && data[1] == 11 && data[2] == 13);
  moved.release(&moved);
  assert(atomic_load(&finalizations) == 1);
  arrow_node_release(&parent);
  arrow_node_release(&moved);
  assert(atomic_load(&finalizations) == 1);
  eng_arrow_owner_free(released_owner);

  /* Independent descriptors may drop on different consumer threads. */
  shared = owner(2);
  struct ArrowArray first = node(shared, data);
  struct ArrowArray second = node(shared, data);
  pthread_t a, b;
  assert(pthread_create(&a, NULL, release_on_worker, &first) == 0);
  assert(pthread_create(&b, NULL, release_on_worker, &second) == 0);
  assert(pthread_join(a, NULL) == 0 && pthread_join(b, NULL) == 0);
  assert(atomic_load(&finalizations) == 2);
  assert(data[0] == 7 && data[1] == 11 && data[2] == 13);
  eng_arrow_owner_free(released_owner);
  puts("C_ARROW_OWNERSHIP_OK: data pointers shared, moved children retained, one final release across threads");
  return 0;
}
